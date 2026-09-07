"""Explicit, later-only execution workflow for a compiled experiment plan."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dashboard.data import load_finalized_run
from iot_energy_pipeline.distributed_fixture import prepare_distributed_fixture
from iot_energy_pipeline.distributed_run import prepare_distributed_run
from iot_energy_pipeline.experiment_plan import (
    _snapshot_artifact,
    build_planned_runs,
)
from iot_energy_pipeline.machine_metadata import validate_machine_metadata
from iot_energy_pipeline.reports import load_experiment_matrix, summarize

EXECUTION_CONFIRMATION = "RUN-DESIGNATED-MACHINE-EXPERIMENTS"


def _safe_run_id(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value
    ):
        raise ValueError("experiment run_id is unsafe")
    return value


@dataclass(frozen=True)
class CommandSpec:
    """One auditable subprocess stage in a distributed experiment run."""

    stage: str
    argv: tuple[str, ...]
    cwd: Path


def require_execution_confirmation(value: str) -> None:
    """Reject accidental execution unless the exact explicit phrase is supplied."""

    if value != EXECUTION_CONFIRMATION:
        raise ValueError(
            f"execution confirmation must equal {EXECUTION_CONFIRMATION!r}"
        )


def _required_text(plan: dict[str, Any], field: str) -> str:
    value = plan.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"distributed plan {field} is invalid")
    return value


def _container_run_path(runtime_root: Path, run_dir: Path) -> str:
    try:
        relative = run_dir.resolve(strict=False).relative_to(
            runtime_root.resolve(strict=False)
        )
    except ValueError as exc:
        raise ValueError("run directory must be below the runtime root") from exc
    if not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError("run directory relative path is invalid")
    posix = PurePosixPath("/workspace/runtime", *relative.parts)
    return str(posix)


def build_processing_commands(
    *,
    repository: Path,
    runtime_root: Path,
    run_dir: Path,
    plan: dict[str, Any],
    python_executable: Path,
    interrupted_recovery: bool,
) -> list[CommandSpec]:
    """Build every processing command after a run has been prepared."""

    run_id = _safe_run_id(_required_text(plan, "run_id"))
    if run_dir.name != run_id:
        raise ValueError("distributed plan run_id does not match its run directory")
    topic = _required_text(plan, "topic")
    replay_hash = _required_text(plan, "replay_manifest_hash")
    canonical_hash = _required_text(plan, "canonical_ndjson_sha256")
    watermark = _required_text(plan, "watermark")
    max_offsets = plan.get("max_offsets_per_trigger")
    if (
        isinstance(max_offsets, bool)
        or not isinstance(max_offsets, int)
        or max_offsets < 1
    ):
        raise ValueError("distributed plan max_offsets_per_trigger is invalid")
    container_run = _container_run_path(runtime_root, run_dir)
    host_python = str(python_executable)
    replay_base = (
        host_python,
        "-m",
        "iot_energy_pipeline.distributed_run",
        "replay",
        "--run-dir",
        str(run_dir),
        "--bootstrap-servers",
        "localhost:29092",
    )
    commands: list[CommandSpec] = []
    if interrupted_recovery:
        commands.extend(
            [
                CommandSpec(
                    "replay_interrupt",
                    (*replay_base, "--interrupt-after", "1"),
                    repository,
                ),
                CommandSpec("replay_resume", (*replay_base, "--resume"), repository),
            ]
        )
    else:
        commands.append(CommandSpec("replay", replay_base, repository))

    spark_submit = (
        "docker",
        "compose",
        "run",
        "--rm",
        "spark-client",
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark:7077",
    )
    canonical_container = f"{container_run}/snapshot/canonical.ndjson"
    commands.extend(
        [
            CommandSpec(
                "spark_stream",
                (
                    *spark_submit,
                    "/opt/pipeline/jobs/spark/stream.py",
                    "--bootstrap-servers",
                    "kafka:9092",
                    "--topic",
                    topic,
                    "--canonical-ndjson",
                    canonical_container,
                    "--output-root",
                    f"{container_run}/layers",
                    "--checkpoint-root",
                    f"{container_run}/checkpoints",
                    "--replay-manifest-hash",
                    replay_hash,
                    "--watermark",
                    watermark,
                    "--max-offsets-per-trigger",
                    str(max_offsets),
                ),
                repository,
            ),
            CommandSpec(
                "spark_gold",
                (
                    *spark_submit,
                    "/opt/pipeline/jobs/spark/finalize_distributed.py",
                    "--run-dir",
                    container_run,
                    "--layers-root",
                    f"{container_run}/layers",
                ),
                repository,
            ),
            CommandSpec(
                "spark_layer_audit",
                (
                    *spark_submit,
                    "/opt/pipeline/jobs/spark/audit_layers.py",
                    "--layers-root",
                    f"{container_run}/layers",
                    "--output-root",
                    f"{container_run}/layer-audit",
                    "--replay-manifest-hash",
                    replay_hash,
                ),
                repository,
            ),
            CommandSpec(
                "hadoop_oracle",
                (
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "hadoop",
                    "python3",
                    "/opt/pipeline/jobs/hadoop/distributed_oracle.py",
                    "--run-id",
                    run_id,
                    "--canonical-ndjson",
                    canonical_container,
                    "--output-dir",
                    f"{container_run}/oracle",
                ),
                repository,
            ),
            CommandSpec(
                "reconciliation",
                (
                    host_python,
                    "-m",
                    "iot_energy_pipeline.run_reconciliation",
                    "--run-dir",
                    str(run_dir),
                    "--oracle-dir",
                    str(run_dir / "oracle"),
                    "--layer-audit-dir",
                    str(run_dir / "layer-audit"),
                    "--output",
                    str(run_dir / "reports/reconciliation.json"),
                ),
                repository,
            ),
            CommandSpec(
                "decision_impact",
                (
                    host_python,
                    "-m",
                    "iot_energy_pipeline.business_report",
                    "--run-dir",
                    str(run_dir),
                    "--oracle-dir",
                    str(run_dir / "oracle"),
                    "--layer-audit-dir",
                    str(run_dir / "layer-audit"),
                    "--reconciliation",
                    str(run_dir / "reports/reconciliation.json"),
                    "--output",
                    str(run_dir / "reports/decision-impact.json"),
                ),
                repository,
            ),
            CommandSpec(
                "storage_benchmark",
                (
                    *spark_submit,
                    "/opt/pipeline/jobs/spark/benchmark_storage.py",
                    "--run-id",
                    run_id,
                    "--canonical-ndjson",
                    canonical_container,
                    "--expected-sha256",
                    canonical_hash,
                    "--output-root",
                    f"{container_run}/storage",
                ),
                repository,
            ),
            CommandSpec(
                "query_benchmark",
                (
                    *spark_submit,
                    "/opt/pipeline/jobs/spark/benchmark_queries.py",
                    "--gold-dir",
                    f"{container_run}/gold",
                    "--decision-report",
                    f"{container_run}/reports/decision-impact.json",
                    "--queries-dir",
                    "/opt/pipeline/queries",
                    "--output",
                    f"{container_run}/reports/query-benchmark.json",
                    "--repetitions",
                    "3",
                ),
                repository,
            ),
            CommandSpec(
                "run_seal",
                (
                    host_python,
                    "-m",
                    "iot_energy_pipeline.run_finalization",
                    "--run-dir",
                    str(run_dir),
                    "--oracle-dir",
                    str(run_dir / "oracle"),
                    "--layer-audit-dir",
                    str(run_dir / "layer-audit"),
                    "--reconciliation",
                    str(run_dir / "reports/reconciliation.json"),
                    "--decision-report",
                    str(run_dir / "reports/decision-impact.json"),
                    "--machine-metadata",
                    str(run_dir / "machine-metadata.json"),
                ),
                repository,
            ),
        ]
    )
    return commands


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _validate_execution_plan(
    plan: dict[str, Any], *, repository: Path, full_event_count: int
) -> list[dict[str, Any]]:
    fields = {
        "schema_version",
        "status",
        "execution_gate",
        "matrix_sha256",
        "snapshot_manifest_sha256",
        "canonical_ndjson_sha256",
        "machine_metadata_sha256",
        "full_event_count",
        "summary_statistics",
        "storage_formats",
        "query_names",
        "normalized_tariff_rates",
        "monetary_interpretation",
        "runs",
    }
    matrix_path = repository / "configs/experiments.toml"
    matrix = load_experiment_matrix(matrix_path)
    if (
        set(plan) != fields
        or plan.get("schema_version") != "experiment-plan-v1"
        or plan.get("status") != "planned_not_executed"
        or plan.get("execution_gate") != "explicit execute command required"
        or plan.get("matrix_sha256") != _sha256(matrix_path)
        or plan.get("full_event_count") != full_event_count
        or plan.get("summary_statistics") != ["median", "minimum", "maximum"]
        or plan.get("storage_formats") != matrix["storage"]["formats"]
        or plan.get("query_names") != matrix["queries"]["names"]
        or plan.get("normalized_tariff_rates") != matrix["business"]["normalized_rates"]
        or plan.get("monetary_interpretation")
        != "hypothetical scenarios; not HKUST results"
    ):
        raise ValueError("experiment plan differs from the fixed compiled policy")
    expected_runs = build_planned_runs(matrix, full_event_count)
    if plan.get("runs") != expected_runs:
        raise ValueError("experiment plan run matrix differs from the fixed policy")
    return expected_runs


def _run_command(
    command: CommandSpec,
    *,
    environment: dict[str, str],
    log_dir: Path,
    sequence: int,
) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{sequence:02d}-{command.stage}"
    stdout_path = log_dir / f"{prefix}.stdout.log"
    stderr_path = log_dir / f"{prefix}.stderr.log"
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        result = subprocess.run(
            command.argv,
            cwd=command.cwd,
            env=environment,
            stdout=stdout,
            stderr=stderr,
            check=False,
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"experiment stage {command.stage} failed with exit code "
            f"{result.returncode}; see {stderr_path}"
        )


def _write_new(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable experiment results already exist: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _prepare_run(
    run: dict[str, Any], snapshot_dir: Path, run_dir: Path
) -> dict[str, Any]:
    event_count = run.get("event_count")
    run_id = str(run["run_id"])
    fault = str(run["fault"])
    seed = int(run["seed"])
    partition_count = int(run["partition_count"])
    watermark = str(run["watermark"])
    max_offsets_per_trigger = int(run["max_offsets_per_trigger"])
    if event_count == "fixture":
        return prepare_distributed_fixture(
            run_dir,
            run_id=run_id,
            fault=fault,
            seed=seed,
            partition_count=partition_count,
            watermark=watermark,
            max_offsets_per_trigger=max_offsets_per_trigger,
        )
    if isinstance(event_count, bool) or not isinstance(event_count, int):
        raise ValueError("planned event_count is invalid")
    return prepare_distributed_run(
        snapshot_dir=snapshot_dir,
        output_dir=run_dir,
        run_id=run_id,
        fault=fault,
        event_count=event_count,
        seed=seed,
        partition_count=partition_count,
        watermark=watermark,
        max_offsets_per_trigger=max_offsets_per_trigger,
    )


def _performance_summary(
    completed: list[dict[str, Any]], runtime_root: Path
) -> list[dict[str, Any]]:
    grouped: dict[int, list[dict[str, float]]] = {}
    for item in completed:
        if item["experiment"] != "scale":
            continue
        run_dir = runtime_root / item["run_id"]
        gold = _json_object(run_dir / "gold/gold-manifest.json", "Gold manifest")
        oracle = _json_object(
            run_dir / "oracle/hadoop-oracle-manifest.json", "Hadoop manifest"
        )
        decision = _json_object(
            run_dir / "reports/decision-impact.json", "decision-impact report"
        )
        grouped.setdefault(int(item["event_count"]), []).append(
            {
                "gold_finalization_seconds": float(gold["gold_finalization_seconds"]),
                "hadoop_verification_seconds": float(oracle["verification_seconds"]),
                "end_to_end_verification_seconds": float(
                    decision["verification_seconds"]
                ),
                "workflow_seconds": float(item["workflow_seconds"]),
            }
        )
    summaries: list[dict[str, Any]] = []
    for event_count, repetitions in sorted(grouped.items()):
        if len(repetitions) != 3:
            raise ValueError(
                f"scale {event_count} requires exactly three completed repetitions"
            )
        summaries.append(
            {
                "event_count": event_count,
                "repetitions": 3,
                "metrics": {
                    name: summarize(row[name] for row in repetitions)
                    for name in repetitions[0]
                },
            }
        )
    return summaries


def _recovery_equivalence(runtime_root: Path) -> dict[str, Any]:
    uninterrupted = runtime_root / "recovery-uninterrupted-r01"
    resumed = runtime_root / "recovery-interrupted_resumed-r01"
    fields = {
        "gold_source_records": "gold/gold-source-events.ndjson",
        "hadoop_source_records": "oracle/hadoop-source-records.ndjson",
        "gold_aggregates": "gold/gold-aggregates.ndjson",
        "hadoop_aggregates": "oracle/hadoop-aggregates.ndjson",
    }
    comparisons = {
        label: {
            "uninterrupted_sha256": _sha256(uninterrupted / relative),
            "resumed_sha256": _sha256(resumed / relative),
        }
        for label, relative in fields.items()
    }
    if any(
        value["uninterrupted_sha256"] != value["resumed_sha256"]
        for value in comparisons.values()
    ):
        raise ValueError("interrupted and uninterrupted recovery results differ")
    return {"status": "identical", "comparisons": comparisons}


def _semantic_hashes(run_dir: Path) -> dict[str, str]:
    fields = {
        "gold_source_records": "gold/gold-source-events.ndjson",
        "gold_aggregates": "gold/gold-aggregates.ndjson",
        "hadoop_source_records": "oracle/hadoop-source-records.ndjson",
        "hadoop_aggregates": "oracle/hadoop-aggregates.ndjson",
    }
    return {label: _sha256(run_dir / relative) for label, relative in fields.items()}


def evaluate_cross_run_acceptance(runtime_root: Path) -> dict[str, Any]:
    """Enforce fault equivalence and the declared restrictive-watermark effect."""

    correctness_ids = [
        f"correctness-{fault}-r01"
        for fault in ("clean", "duplicate", "delayed", "malformed", "combined")
    ]
    correctness_hashes = {
        run_id: _semantic_hashes(runtime_root / run_id) for run_id in correctness_ids
    }
    reference = correctness_hashes[correctness_ids[0]]
    if any(value != reference for value in correctness_hashes.values()):
        raise ValueError("fault schedules produced different semantic final records")

    retaining_dir = runtime_root / "watermark-retaining-r01"
    restrictive_dir = runtime_root / "watermark-restrictive-r01"
    retaining_hashes = _semantic_hashes(retaining_dir)
    restrictive_hashes = _semantic_hashes(restrictive_dir)
    if retaining_hashes != restrictive_hashes:
        raise ValueError(
            "watermark configurations produced different exact Gold/oracle"
        )
    retaining_impact = _json_object(
        retaining_dir / "reports/decision-impact.json",
        "retaining-watermark decision report",
    )
    restrictive_impact = _json_object(
        restrictive_dir / "reports/decision-impact.json",
        "restrictive-watermark decision report",
    )
    retaining_count = retaining_impact.get("affected_meter_period_count")
    restrictive_count = restrictive_impact.get("affected_meter_period_count")
    if (
        isinstance(retaining_count, bool)
        or not isinstance(retaining_count, int)
        or isinstance(restrictive_count, bool)
        or not isinstance(restrictive_count, int)
        or restrictive_count <= retaining_count
    ):
        raise ValueError(
            "restrictive watermark did not expose an additional candidate difference"
        )
    return {
        "correctness_semantic_equivalence": {
            "status": "identical",
            "reference_run_id": correctness_ids[0],
            "run_hashes": correctness_hashes,
        },
        "watermark_effect": {
            "gold_status": "identical",
            "candidate_difference_exposed": True,
            "retaining_affected_meter_periods": retaining_count,
            "restrictive_affected_meter_periods": restrictive_count,
            "semantic_hashes": retaining_hashes,
        },
    }


def execute_experiment_plan(
    *,
    experiment_plan_path: Path,
    snapshot_dir: Path,
    machine_metadata_path: Path,
    runtime_root: Path,
    repository: Path,
    python_executable: Path,
    confirmation: str,
) -> dict[str, Any]:
    """Execute the compiled matrix only after an exact designated-machine gate."""

    require_execution_confirmation(confirmation)
    repository = repository.resolve(strict=True)
    runtime_root = runtime_root.resolve(strict=False)
    try:
        runtime_root.relative_to(repository)
    except ValueError as exc:
        raise ValueError("runtime root must remain inside the repository") from exc
    plan = _json_object(experiment_plan_path, "experiment plan")
    machine = _json_object(machine_metadata_path, "machine metadata")
    validate_machine_metadata(machine)
    if machine.get("execution_context") != "designated_machine":
        raise ValueError("experiment execution requires designated_machine metadata")
    if (
        plan.get("schema_version") != "experiment-plan-v1"
        or plan.get("status") != "planned_not_executed"
    ):
        raise ValueError("experiment plan is not executable")
    if plan.get("machine_metadata_sha256") != _sha256(machine_metadata_path):
        raise ValueError("experiment plan machine metadata binding mismatch")
    snapshot_manifest_path = snapshot_dir / "snapshot-manifest.json"
    if plan.get("snapshot_manifest_sha256") != _sha256(snapshot_manifest_path):
        raise ValueError("experiment plan snapshot binding mismatch")
    snapshot_manifest = _json_object(snapshot_manifest_path, "snapshot manifest")
    artifacts = snapshot_manifest.get("artifacts")
    canonical_descriptor = (
        artifacts.get("canonical_ndjson") if isinstance(artifacts, dict) else None
    )
    if not isinstance(canonical_descriptor, dict):
        raise ValueError("snapshot canonical descriptor is invalid")
    canonical_path = _snapshot_artifact(snapshot_manifest_path, canonical_descriptor)
    if (
        not canonical_path.is_file()
        or canonical_path.is_symlink()
        or plan.get("canonical_ndjson_sha256") != _sha256(canonical_path)
    ):
        raise ValueError("experiment plan canonical snapshot binding mismatch")
    full_event_count = canonical_descriptor.get("record_count")
    if (
        isinstance(full_event_count, bool)
        or not isinstance(full_event_count, int)
        or full_event_count < 1
    ):
        raise ValueError("snapshot canonical record count is invalid")
    runs = _validate_execution_plan(
        plan,
        repository=repository,
        full_event_count=full_event_count,
    )
    run_ids: list[str] = []
    for run in runs:
        run_id = run.get("run_id") if isinstance(run, dict) else None
        try:
            run_ids.append(_safe_run_id(run_id))
        except ValueError as exc:
            raise ValueError("experiment plan run IDs are invalid") from exc
    if len(run_ids) != len(set(run_ids)):
        raise ValueError("experiment plan run IDs are invalid")
    results_path = runtime_root / "experiment-results.json"
    if results_path.exists():
        raise FileExistsError(
            f"immutable experiment results already exist: {results_path}"
        )
    existing = [run_id for run_id in run_ids if (runtime_root / run_id).exists()]
    if existing:
        raise FileExistsError(f"one or more planned run directories exist: {existing}")

    runtime_root.mkdir(parents=True, exist_ok=True)
    orchestration_logs = runtime_root / "_orchestration-logs"
    if orchestration_logs.exists():
        raise FileExistsError("experiment orchestration log directory already exists")
    environment = dict(os.environ)
    environment["PIPELINE_RUNTIME_DIR"] = str(runtime_root)
    start = CommandSpec(
        "services_start",
        (
            "docker",
            "compose",
            "up",
            "-d",
            "--build",
            "kafka",
            "spark",
            "spark-worker",
            "hdfs",
            "hadoop",
        ),
        repository,
    )
    stop = CommandSpec("services_stop", ("docker", "compose", "stop"), repository)
    services_started = False
    completed: list[dict[str, Any]] = []
    try:
        _run_command(
            start,
            environment=environment,
            log_dir=orchestration_logs / "services",
            sequence=1,
        )
        services_started = True
        for run_number, raw_run in enumerate(runs, start=1):
            if not isinstance(raw_run, dict):
                raise ValueError("experiment plan run is not an object")
            run_id = str(raw_run["run_id"])
            run_dir = runtime_root / run_id
            started = time.perf_counter()
            distributed_plan = _prepare_run(raw_run, snapshot_dir, run_dir)
            machine_target = run_dir / "machine-metadata.json"
            if machine_target.exists():
                raise FileExistsError("run machine metadata already exists")
            shutil.copyfile(machine_metadata_path, machine_target)
            commands = build_processing_commands(
                repository=repository,
                runtime_root=runtime_root,
                run_dir=run_dir,
                plan=distributed_plan,
                python_executable=python_executable,
                interrupted_recovery=(raw_run.get("mode") == "interrupted_resumed"),
            )
            for sequence, command in enumerate(commands, start=1):
                _run_command(
                    command,
                    environment=environment,
                    log_dir=orchestration_logs / f"{run_number:02d}-{run_id}",
                    sequence=sequence,
                )
            finalized = load_finalized_run(run_dir)
            completed.append(
                {
                    "run_id": run_id,
                    "experiment": raw_run["experiment"],
                    "event_count": raw_run["event_count"],
                    "repetition": raw_run["repetition"],
                    "status": "finalized",
                    "workflow_seconds": time.perf_counter() - started,
                    "run_manifest_sha256": _sha256(run_dir / "run-manifest.json"),
                    "artifact_count": len(finalized["artifacts"]),
                }
            )
    finally:
        if services_started:
            _run_command(
                stop,
                environment=environment,
                log_dir=orchestration_logs / "services",
                sequence=2,
            )
    if len(completed) != len(runs):
        raise ValueError("not every planned experiment run finalized")
    cross_run = evaluate_cross_run_acceptance(runtime_root)
    results: dict[str, Any] = {
        "schema_version": "experiment-results-v1",
        "status": "complete",
        "experiment_plan_sha256": _sha256(experiment_plan_path),
        "machine_metadata_sha256": _sha256(machine_metadata_path),
        "snapshot_manifest_sha256": _sha256(snapshot_manifest_path),
        "runs": completed,
        "performance_summary": _performance_summary(completed, runtime_root),
        "correctness_semantic_equivalence": cross_run[
            "correctness_semantic_equivalence"
        ],
        "recovery_semantic_equivalence": _recovery_equivalence(runtime_root),
        "watermark_effect": cross_run["watermark_effect"],
        "monetary_interpretation": "hypothetical scenarios; not HKUST results",
    }
    _write_new(results_path, results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-plan", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument("--machine-metadata", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--confirm", required=True)
    args = parser.parse_args()
    result = execute_experiment_plan(
        experiment_plan_path=args.experiment_plan,
        snapshot_dir=args.snapshot_dir,
        machine_metadata_path=args.machine_metadata,
        runtime_root=args.runtime_root,
        repository=args.repository,
        python_executable=args.python,
        confirmation=args.confirm,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

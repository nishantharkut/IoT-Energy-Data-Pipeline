"""Compile the fixed coursework matrix without executing any workload."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any, cast

from iot_energy_pipeline.machine_metadata import validate_machine_metadata
from iot_energy_pipeline.reports import build_experiment_runs, load_experiment_matrix


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


def _snapshot_artifact(manifest_path: Path, descriptor: object) -> Path:
    if not isinstance(descriptor, dict):
        raise ValueError("canonical snapshot descriptor is invalid")
    relative = descriptor.get("file")
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("canonical snapshot path is unsafe")
    posix = PurePosixPath(relative)
    if posix.is_absolute() or any(part in {"", ".", ".."} for part in posix.parts):
        raise ValueError("canonical snapshot path is unsafe")
    root = manifest_path.parent
    path = root.joinpath(*posix.parts)
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise ValueError("canonical snapshot path is unsafe") from exc
    if path.is_symlink() or _sha256(path) != descriptor.get("sha256"):
        raise ValueError("canonical snapshot integrity check failed")
    return path


def _write_new(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable experiment plan already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def build_planned_runs(
    matrix: dict[str, Any], full_event_count: int
) -> list[dict[str, Any]]:
    """Resolve the execution fields added to every fixed matrix run."""

    planned_runs: list[dict[str, Any]] = []
    for run in build_experiment_runs(matrix, full_event_count):
        fixture = run["event_count"] == "fixture"
        max_offsets_per_trigger = (
            1 if run["experiment"] == "watermark" else (60 if fixture else 10000)
        )
        planned_runs.append(
            {
                **run,
                "watermark": run.get("watermark", matrix["watermark"]["retaining"]),
                "max_offsets_per_trigger": max_offsets_per_trigger,
                "partition_count": 1 if run["experiment"] == "watermark" else 3,
                "seed": 20260825,
                "status": "pending",
                "publication_requirements": [
                    "Spark-Hadoop reconciliation passed",
                    "decision-impact report verified",
                    "finalized run integrity seal present",
                ],
                "live_demo_eligible": (
                    fixture
                    or (
                        isinstance(run["event_count"], int)
                        and run["event_count"]
                        <= matrix["execution"]["live_demo_max_events"]
                    )
                ),
            }
        )
    return planned_runs


def compile_experiment_plan(
    *,
    matrix_path: Path,
    snapshot_manifest_path: Path,
    machine_metadata_path: Path,
    output: Path,
) -> dict[str, Any]:
    """Bind the complete matrix to snapshot and machine evidence; execute nothing."""

    if output.exists():
        raise FileExistsError(f"immutable experiment plan already exists: {output}")
    matrix = load_experiment_matrix(matrix_path)
    snapshot = _json_object(snapshot_manifest_path, "snapshot manifest")
    machine = _json_object(machine_metadata_path, "machine metadata")
    validate_machine_metadata(machine)
    if machine.get("execution_context") != "designated_machine":
        raise ValueError("full experiment plans require designated_machine metadata")
    if (
        snapshot.get("schema_version") != "snapshot-manifest-v1"
        or snapshot.get("status") != "complete"
    ):
        raise ValueError("snapshot manifest is not complete")
    artifacts = snapshot.get("artifacts")
    descriptor = (
        artifacts.get("canonical_ndjson") if isinstance(artifacts, dict) else None
    )
    canonical_path = _snapshot_artifact(snapshot_manifest_path, descriptor)
    descriptor = cast(dict[str, Any], descriptor)
    full_count = descriptor.get("record_count")
    if isinstance(full_count, bool) or not isinstance(full_count, int):
        raise ValueError("canonical snapshot record count is invalid")
    planned_runs = build_planned_runs(matrix, full_count)
    plan: dict[str, Any] = {
        "schema_version": "experiment-plan-v1",
        "status": "planned_not_executed",
        "execution_gate": "explicit execute command required",
        "matrix_sha256": _sha256(matrix_path),
        "snapshot_manifest_sha256": _sha256(snapshot_manifest_path),
        "canonical_ndjson_sha256": _sha256(canonical_path),
        "machine_metadata_sha256": _sha256(machine_metadata_path),
        "full_event_count": full_count,
        "summary_statistics": ["median", "minimum", "maximum"],
        "storage_formats": matrix["storage"]["formats"],
        "query_names": matrix["queries"]["names"],
        "normalized_tariff_rates": matrix["business"]["normalized_rates"],
        "monetary_interpretation": "hypothetical scenarios; not HKUST results",
        "runs": planned_runs,
    }
    _write_new(output, plan)
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compile the coursework matrix without executing workloads"
    )
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--snapshot-manifest", type=Path, required=True)
    parser.add_argument("--machine-metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = compile_experiment_plan(
        matrix_path=args.matrix,
        snapshot_manifest_path=args.snapshot_manifest,
        machine_metadata_path=args.machine_metadata,
        output=args.output,
    )
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

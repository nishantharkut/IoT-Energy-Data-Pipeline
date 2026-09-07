from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from iot_energy_pipeline.experiment_execution import (
    EXECUTION_CONFIRMATION,
    _safe_run_id,
    _validate_execution_plan,
    build_processing_commands,
    evaluate_cross_run_acceptance,
    execute_experiment_plan,
    require_execution_confirmation,
)
from iot_energy_pipeline.experiment_plan import build_planned_runs
from iot_energy_pipeline.reports import load_experiment_matrix


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _plan() -> dict:
    return {
        "run_id": "scale-100000-r01",
        "topic": "iot-energy-scale-100000-r01",
        "replay_manifest_hash": "a" * 64,
        "canonical_ndjson_sha256": "b" * 64,
        "watermark": "24 hours",
        "max_offsets_per_trigger": 10000,
    }


def _fixed_execution_plan(full_event_count: int = 3_250_000) -> dict:
    matrix_path = Path("configs/experiments.toml")
    matrix = load_experiment_matrix(matrix_path)
    return {
        "schema_version": "experiment-plan-v1",
        "status": "planned_not_executed",
        "execution_gate": "explicit execute command required",
        "matrix_sha256": hashlib.sha256(matrix_path.read_bytes()).hexdigest(),
        "snapshot_manifest_sha256": "a" * 64,
        "canonical_ndjson_sha256": "b" * 64,
        "machine_metadata_sha256": "c" * 64,
        "full_event_count": full_event_count,
        "summary_statistics": ["median", "minimum", "maximum"],
        "storage_formats": matrix["storage"]["formats"],
        "query_names": matrix["queries"]["names"],
        "normalized_tariff_rates": matrix["business"]["normalized_rates"],
        "monetary_interpretation": "hypothetical scenarios; not HKUST results",
        "runs": build_planned_runs(matrix, full_event_count),
    }


def test_execution_requires_exact_designated_machine_confirmation() -> None:
    with pytest.raises(ValueError, match="confirmation"):
        require_execution_confirmation("yes")

    require_execution_confirmation(EXECUTION_CONFIRMATION)

    with pytest.raises(ValueError, match="confirmation"):
        execute_experiment_plan(
            experiment_plan_path=Path("missing-plan.json"),
            snapshot_dir=Path("missing-snapshot"),
            machine_metadata_path=Path("missing-machine.json"),
            runtime_root=Path("missing-runtime"),
            repository=Path.cwd(),
            python_executable=Path("python"),
            confirmation="no",
        )


@pytest.mark.parametrize(
    "run_id",
    ("../escape", "nested/run", "nested\\run", ".", "a" * 129),
)
def test_execution_rejects_unsafe_run_ids_before_preparation(run_id: str) -> None:
    with pytest.raises(ValueError, match="run_id"):
        _safe_run_id(run_id)

    assert _safe_run_id("correctness-clean-r01") == "correctness-clean-r01"


def test_execution_rederives_and_validates_the_fixed_compiled_plan() -> None:
    plan = _fixed_execution_plan()

    assert (
        _validate_execution_plan(
            plan, repository=Path.cwd(), full_event_count=3_250_000
        )
        == plan["runs"]
    )

    changed = {**plan, "runs": [dict(run) for run in plan["runs"]]}
    changed["runs"][0]["fault"] = "delayed"
    with pytest.raises(ValueError, match="run matrix"):
        _validate_execution_plan(
            changed, repository=Path.cwd(), full_event_count=3_250_000
        )

    with pytest.raises(ValueError, match="compiled policy"):
        _validate_execution_plan(
            {**plan, "matrix_sha256": "0" * 64},
            repository=Path.cwd(),
            full_event_count=3_250_000,
        )


def test_processing_commands_cover_every_fail_closed_stage(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    runtime = repository / "reports/runtime"
    run = runtime / "scale-100000-r01"
    commands = build_processing_commands(
        repository=repository,
        runtime_root=runtime,
        run_dir=run,
        plan=_plan(),
        python_executable=Path("python"),
        interrupted_recovery=False,
    )

    assert [command.stage for command in commands] == [
        "replay",
        "spark_stream",
        "spark_gold",
        "spark_layer_audit",
        "hadoop_oracle",
        "reconciliation",
        "decision_impact",
        "storage_benchmark",
        "query_benchmark",
        "run_seal",
    ]
    rendered = [" ".join(str(part) for part in command.argv) for command in commands]
    assert all("F:\\" not in command for command in rendered)
    assert (
        sum("/workspace/runtime/scale-100000-r01" in command for command in rendered)
        >= 5
    )
    assert "available" not in rendered[0]
    assert "iot_energy_pipeline.run_finalization" in rendered[-1]
    decision = next(
        command for command in commands if command.stage == "decision_impact"
    )
    assert decision.argv.count("--oracle-dir") == 1


def test_interrupted_recovery_has_interrupt_then_resume_replay(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    runtime = repository / "reports/runtime"
    commands = build_processing_commands(
        repository=repository,
        runtime_root=runtime,
        run_dir=runtime / "recovery-interrupted_resumed-r01",
        plan={**_plan(), "run_id": "recovery-interrupted_resumed-r01"},
        python_executable=Path("python"),
        interrupted_recovery=True,
    )

    assert [command.stage for command in commands[:2]] == [
        "replay_interrupt",
        "replay_resume",
    ]
    assert "--interrupt-after" in commands[0].argv
    assert "--resume" in commands[1].argv


def test_cross_run_acceptance_requires_fault_equivalence_and_watermark_effect(
    tmp_path: Path,
) -> None:
    payloads = {
        "gold/gold-source-events.ndjson": '{"source":1}\n',
        "gold/gold-aggregates.ndjson": '{"aggregate":1}\n',
        "oracle/hadoop-source-records.ndjson": '{"source":1}\n',
        "oracle/hadoop-aggregates.ndjson": '{"aggregate":1}\n',
    }
    for fault in ("clean", "duplicate", "delayed", "malformed", "combined"):
        root = tmp_path / f"correctness-{fault}-r01"
        for relative, content in payloads.items():
            _write(root / relative, content)
    for mode, affected in (("retaining", 0), ("restrictive", 2)):
        root = tmp_path / f"watermark-{mode}-r01"
        for relative, content in payloads.items():
            _write(root / relative, content)
        _write(
            root / "reports/decision-impact.json",
            json.dumps(
                {
                    "affected_meter_period_count": affected,
                    "absolute_kwh_discrepancy": str(affected),
                }
            ),
        )

    result = evaluate_cross_run_acceptance(tmp_path)

    assert result["correctness_semantic_equivalence"]["status"] == "identical"
    assert result["watermark_effect"]["gold_status"] == "identical"
    assert result["watermark_effect"]["candidate_difference_exposed"] is True

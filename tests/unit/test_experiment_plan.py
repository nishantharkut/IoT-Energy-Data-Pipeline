from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from iot_energy_pipeline.experiment_plan import compile_experiment_plan


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")


def _inputs(tmp_path: Path, context: str = "designated_machine") -> tuple[Path, Path]:
    snapshot = tmp_path / "snapshot"
    canonical = snapshot / "canonical.ndjson"
    canonical.parent.mkdir(parents=True)
    canonical.write_text("{}\n", encoding="utf-8")
    _write_json(
        snapshot / "snapshot-manifest.json",
        {
            "schema_version": "snapshot-manifest-v1",
            "status": "complete",
            "artifacts": {
                "canonical_ndjson": {
                    "file": "canonical.ndjson",
                    "record_count": 3_250_000,
                    "sha256": hashlib.sha256(canonical.read_bytes()).hexdigest(),
                }
            },
        },
    )
    machine = tmp_path / "machine.json"
    _write_json(
        machine,
        {
            "schema_version": "machine-metadata-v1",
            "execution_context": context,
            "captured_at_utc": "2026-08-26T10:00:00Z",
            "hostname": "designated-host",
            "operating_system": "Windows",
            "processor": "cpu",
            "logical_cpu_count": 8,
            "memory_bytes": 16_000_000_000,
            "software": {
                "python": "3.11.9",
                "docker": "27",
                "kafka": "3.7.1",
                "spark": "3.5.1",
                "hadoop": "3.4.3",
            },
        },
    )
    return snapshot / "snapshot-manifest.json", machine


def test_experiment_plan_compiles_complete_deferred_matrix(tmp_path: Path) -> None:
    snapshot, machine = _inputs(tmp_path)
    output = tmp_path / "experiment-plan.json"

    plan = compile_experiment_plan(
        matrix_path=Path("configs/experiments.toml"),
        snapshot_manifest_path=snapshot,
        machine_metadata_path=machine,
        output=output,
    )

    assert plan["status"] == "planned_not_executed"
    assert plan["full_event_count"] == 3_250_000
    assert len(plan["runs"]) == 18
    assert len({run["run_id"] for run in plan["runs"]}) == 18
    assert [
        run["event_count"] for run in plan["runs"] if run["experiment"] == "scale"
    ] == [
        100_000,
        100_000,
        100_000,
        1_000_000,
        1_000_000,
        1_000_000,
        3_250_000,
        3_250_000,
        3_250_000,
    ]
    assert plan["summary_statistics"] == ["median", "minimum", "maximum"]
    assert plan["execution_gate"] == "explicit execute command required"
    assert all(run["watermark"] for run in plan["runs"])
    assert all(run["max_offsets_per_trigger"] > 0 for run in plan["runs"])
    watermark_runs = [run for run in plan["runs"] if run["experiment"] == "watermark"]
    assert {run["max_offsets_per_trigger"] for run in watermark_runs} == {1}
    assert {run["partition_count"] for run in watermark_runs} == {1}
    assert json.loads(output.read_text("utf-8")) == plan


def test_experiment_plan_requires_designated_machine_metadata(tmp_path: Path) -> None:
    snapshot, machine = _inputs(tmp_path, context="fixture")

    with pytest.raises(ValueError, match="designated_machine"):
        compile_experiment_plan(
            matrix_path=Path("configs/experiments.toml"),
            snapshot_manifest_path=snapshot,
            machine_metadata_path=machine,
            output=tmp_path / "experiment-plan.json",
        )


def test_experiment_plan_is_immutable(tmp_path: Path) -> None:
    snapshot, machine = _inputs(tmp_path)
    output = tmp_path / "experiment-plan.json"
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="immutable experiment plan"):
        compile_experiment_plan(
            matrix_path=Path("configs/experiments.toml"),
            snapshot_manifest_path=snapshot,
            machine_metadata_path=machine,
            output=output,
        )

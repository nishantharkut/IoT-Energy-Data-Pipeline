from __future__ import annotations

from pathlib import Path


def test_windows_experiment_script_compiles_bound_plan_without_starting_services() -> (
    None
):
    script = Path("scripts/run_experiments.ps1").read_text(encoding="utf-8")

    for parameter in ("Matrix", "SnapshotManifest", "MachineMetadata", "Output"):
        assert f"${parameter}" in script
    assert "iot_energy_pipeline.experiment_plan" in script
    assert "docker compose up" not in script
    assert "$LASTEXITCODE" in script


def test_posix_experiment_script_compiles_bound_plan_without_starting_services() -> (
    None
):
    script = Path("scripts/run_experiments.sh").read_text(encoding="utf-8")

    assert 'if [ "$#" -ne 3 ]' in script
    assert "iot_energy_pipeline.experiment_plan" in script
    assert "configs/experiments.toml" in script
    assert "docker compose up" not in script


def test_execution_wrappers_expose_the_hard_confirmation_gate() -> None:
    windows = Path("scripts/execute_experiments.ps1").read_text(encoding="utf-8")
    posix = Path("scripts/execute_experiments.sh").read_text(encoding="utf-8")

    for script in (windows, posix):
        assert "iot_energy_pipeline.experiment_execution" in script
        assert "RUN-DESIGNATED-MACHINE-EXPERIMENTS" in script
        assert "--confirm" in script

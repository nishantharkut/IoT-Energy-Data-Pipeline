from __future__ import annotations

from pathlib import Path

import pytest

from iot_energy_pipeline.reports import (
    build_experiment_runs,
    load_experiment_matrix,
    resolve_scale_sizes,
)


def test_fixed_experiment_matrix_covers_all_declared_dimensions() -> None:
    matrix = load_experiment_matrix(Path("configs/experiments.toml"))

    assert matrix["correctness"]["faults"] == [
        "clean",
        "duplicate",
        "delayed",
        "malformed",
        "combined",
    ]
    assert matrix["scale"]["requested_sizes"] == [100_000, 1_000_000, 5_000_000]
    assert matrix["scale"]["repetitions"] == 3
    assert matrix["recovery"]["modes"] == ["uninterrupted", "interrupted_resumed"]
    assert set(matrix["watermark"]) >= {"retaining", "restrictive"}
    assert matrix["storage"]["formats"] == [
        "ndjson",
        "parquet_zstd_unpartitioned",
        "parquet_zstd_partitioned",
    ]
    assert matrix["queries"]["names"] == [
        "meter",
        "building",
        "time_window",
        "quality",
        "exposure",
    ]
    assert matrix["business"]["normalized_rates"] == [0.5, 1.0, 2.0]
    assert matrix["execution"]["require_reconciliation"] is True
    assert matrix["execution"]["require_machine_metadata"] is True


def test_final_scale_is_minimum_of_five_million_and_full_dataset() -> None:
    assert resolve_scale_sizes(9_000_000) == [100_000, 1_000_000, 5_000_000]
    assert resolve_scale_sizes(3_250_000) == [100_000, 1_000_000, 3_250_000]
    assert resolve_scale_sizes(1_000_000) == [100_000, 1_000_000]
    with pytest.raises(ValueError, match="at least 1,000,000"):
        resolve_scale_sizes(900_000)


def test_scale_runs_have_three_clean_repetitions_and_stable_ids() -> None:
    matrix = load_experiment_matrix(Path("configs/experiments.toml"))

    first = build_experiment_runs(matrix, 3_250_000)
    second = build_experiment_runs(matrix, 3_250_000)

    assert first == second
    scale = [run for run in first if run["experiment"] == "scale"]
    assert len(scale) == 9
    assert all(run["fault"] == "clean" for run in scale)
    assert {run["repetition"] for run in scale} == {1, 2, 3}
    correctness = [run for run in first if run["experiment"] == "correctness"]
    assert len(correctness) == 5


@pytest.mark.parametrize(
    "old,new",
    (
        ('restrictive = "1 minute"', 'restrictive = "2 minutes"'),
        ("fixture_only = true", "fixture_only = false"),
        (
            'final_size_rule = "min(5000000, full_dataset)"',
            'final_size_rule = "full_dataset"',
        ),
        (
            'formats = ["ndjson", "parquet_zstd_unpartitioned", '
            '"parquet_zstd_partitioned"]',
            'formats = ["ndjson"]',
        ),
        (
            'names = ["meter", "building", "time_window", "quality", "exposure"]',
            'names = ["meter"]',
        ),
        ("live_demo_max_events = 100000", "live_demo_max_events = 1000000"),
    ),
)
def test_fixed_matrix_rejects_policy_drift(tmp_path: Path, old: str, new: str) -> None:
    source = Path("configs/experiments.toml").read_text(encoding="utf-8")
    assert old in source
    changed = tmp_path / "experiments.toml"
    changed.write_text(source.replace(old, new), encoding="utf-8")

    with pytest.raises(ValueError, match="matrix"):
        load_experiment_matrix(changed)

from __future__ import annotations

import json
from pathlib import Path

from dashboard.data import load_finalized_run
from iot_energy_pipeline.fixture import run_local_fixture


def _ndjson(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def test_one_fixture_flow_reaches_every_semantic_acceptance_layer(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "combined-run"

    manifest = run_local_fixture(
        run_dir,
        run_id="fixture-combined",
        fault="combined",
        watermark_seconds=60,
    )

    loaded = load_finalized_run(run_dir)
    assert loaded == manifest
    assert manifest["status"] == "finalized"
    assert manifest["reconciled"] is True
    counts = manifest["views"]["throughput_layers"]["rows"][0]
    assert counts["bronze_deliveries"] == (counts["parse_valid"] + counts["quarantine"])
    assert counts["quarantine"] > 0
    assert counts["late_valid"] > 0
    gold = _ndjson(run_dir / "gold/gold-aggregates.ndjson")
    oracle = _ndjson(run_dir / "oracle/hadoop-aggregates.ndjson")
    assert gold == oracle
    assert _ndjson(run_dir / "gold/gold-source-events.ndjson") == _ndjson(
        run_dir / "oracle/hadoop-source-records.ndjson"
    )
    reconciliation = json.loads(
        (run_dir / "reports/reconciliation.json").read_text("utf-8")
    )
    assert reconciliation["status"] == "passed"
    impact = json.loads((run_dir / "reports/decision-impact.json").read_text("utf-8"))
    assert impact["affected_meter_period_count"] > 0
    assert impact["absolute_kwh_discrepancy"] != "0"
    assert all(
        row["residual_exposure_after_verification"] == "0"
        for row in impact["tariff_scenarios"]
    )


def test_interrupted_and_uninterrupted_runs_are_semantically_identical(
    tmp_path: Path,
) -> None:
    uninterrupted = tmp_path / "uninterrupted"
    resumed = tmp_path / "resumed"

    run_local_fixture(
        uninterrupted,
        run_id="fixture-uninterrupted",
        fault="combined",
        watermark_seconds=24 * 60 * 60,
    )
    run_local_fixture(
        resumed,
        run_id="fixture-resumed",
        fault="combined",
        watermark_seconds=24 * 60 * 60,
        interrupt_after=37,
    )

    for relative in (
        "gold/gold-source-events.ndjson",
        "gold/gold-aggregates.ndjson",
        "oracle/hadoop-aggregates.ndjson",
        "oracle/hadoop-source-records.ndjson",
    ):
        assert _ndjson(uninterrupted / relative) == _ndjson(resumed / relative)
    assert (
        json.loads((uninterrupted / "reports/reconciliation.json").read_text("utf-8"))[
            "status"
        ]
        == "passed"
    )
    assert (
        json.loads((resumed / "reports/reconciliation.json").read_text("utf-8"))[
            "status"
        ]
        == "passed"
    )

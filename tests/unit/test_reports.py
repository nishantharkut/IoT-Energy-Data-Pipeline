from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from iot_energy_pipeline.reports import (
    build_decision_impact_report,
    build_decision_impact_report_from_ndjson,
    derive_interval_consumption,
    summarize,
)


def _reading(
    index: int,
    value: int,
    *,
    time: str | None = None,
    kind: str = "cumulative_energy",
    unit: str = "kWh",
) -> dict:
    return {
        "source_event_id": f"{index:064x}",
        "meter_id": "meter-a",
        "event_time_utc": time or f"2024-01-01T0{index}:00:00Z",
        "scaled_value": value,
        "decimal_scale": 2,
        "measurement_kind": kind,
        "unit": unit,
    }


def test_interval_derivation_handles_zero_and_negative_deltas_conservatively() -> None:
    result = derive_interval_consumption(
        [
            _reading(0, 1000),
            _reading(1, 1250),
            _reading(2, 1250),
            _reading(3, 1200),
            _reading(4, 1300),
        ]
    )

    assert [row["delta_scaled"] for row in result.intervals] == [250, 0, 100]
    assert [row["delta_kwh"] for row in result.intervals] == ["2.5", "0", "1"]
    assert {row["category"] for row in result.flagged_records} == {
        "first_reading_no_interval",
        "negative_delta_or_reset",
    }


def test_duplicate_timestamp_is_flagged_and_not_used_as_a_baseline() -> None:
    result = derive_interval_consumption(
        [
            _reading(0, 1000),
            _reading(1, 1100, time="2024-01-01T01:00:00Z"),
            _reading(2, 1200, time="2024-01-01T01:00:00Z"),
            _reading(3, 1300, time="2024-01-01T02:00:00Z"),
            _reading(4, 1400, time="2024-01-01T03:00:00Z"),
        ]
    )

    assert [row["delta_scaled"] for row in result.intervals] == [100]
    assert result.intervals[0]["interval_end_utc"] == "2024-01-01T03:00:00Z"
    categories = [row["category"] for row in result.flagged_records]
    assert categories.count("ambiguous_duplicate_timestamp") == 2
    assert "first_after_ambiguous_timestamp" in categories


@pytest.mark.parametrize(
    "kind,unit",
    [("UNKNOWN", "kWh"), ("power", "kW"), ("cumulative_energy", "UNKNOWN")],
)
def test_unverified_semantics_never_produce_consumption(kind: str, unit: str) -> None:
    result = derive_interval_consumption(
        [_reading(0, 100, kind=kind, unit=unit), _reading(1, 200, kind=kind, unit=unit)]
    )
    assert result.intervals == ()
    assert all(
        row["category"] == "unverified_cumulative_kwh_semantics"
        for row in result.flagged_records
    )


def test_decision_impact_report_labels_hypothetical_scenarios_and_residual() -> None:
    verified = [_reading(0, 1000), _reading(1, 1100), _reading(2, 1200)]
    candidate = [verified[0], verified[2]]

    report = build_decision_impact_report(
        candidate,
        verified,
        reconciliation_passed=True,
        verification_seconds=4.25,
        candidate_storage_bytes=1000,
        verification_storage_bytes=250,
    )

    assert report["affected_meter_period_count"] == 2
    assert report["absolute_kwh_discrepancy"] == "2"
    assert report["verification_seconds"] == 4.25
    assert report["storage_overhead_bytes"] == 250
    assert report["storage_overhead_ratio"] == 0.25
    assert [row["normalized_rate"] for row in report["tariff_scenarios"]] == [
        "0.5",
        "1.0",
        "2.0",
    ]
    assert all(
        row["label"] == "hypothetical normalized tariff scenario"
        and row["residual_exposure_after_verification"] == "0"
        for row in report["tariff_scenarios"]
    )


def test_business_report_refuses_unreconciled_results() -> None:
    with pytest.raises(ValueError, match="passing Spark-Hadoop reconciliation"):
        build_decision_impact_report(
            [_reading(0, 1)],
            [_reading(0, 1)],
            reconciliation_passed=False,
            verification_seconds=1,
            candidate_storage_bytes=1,
            verification_storage_bytes=1,
        )


def test_disk_backed_decision_report_matches_exact_interval_semantics(
    tmp_path: Path,
) -> None:
    verified = [_reading(0, 1000), _reading(1, 1100), _reading(2, 1200)]
    candidate = [verified[0], verified[2]]
    candidate_path = tmp_path / "candidate.ndjson"
    verified_path = tmp_path / "verified.ndjson"
    for path, records in ((candidate_path, candidate), (verified_path, verified)):
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )

    report = build_decision_impact_report_from_ndjson(
        candidate_path,
        verified_path,
        candidate_record_count=2,
        verified_record_count=3,
        scratch_directory=tmp_path,
        reconciliation_passed=True,
        verification_seconds=4.25,
        candidate_storage_bytes=1000,
        verification_storage_bytes=250,
    )

    assert report["affected_meter_period_count"] == 2
    assert report["absolute_kwh_discrepancy"] == "2"
    assert report["candidate_flagged_record_count"] == 1
    assert report["verified_flagged_record_count"] == 1


def test_disk_backed_report_matches_in_memory_duplicate_and_reset_rules(
    tmp_path: Path,
) -> None:
    verified = [
        _reading(0, 1000),
        _reading(1, 1100, time="2024-01-01T01:00:00Z"),
        _reading(2, 1200, time="2024-01-01T01:00:00Z"),
        _reading(3, 900, time="2024-01-01T02:00:00Z"),
        _reading(4, 1000, time="2024-01-01T03:00:00Z"),
    ]
    candidate = [verified[index] for index in (0, 1, 3, 4)]
    paths = (tmp_path / "candidate.ndjson", tmp_path / "verified.ndjson")
    for path, records in zip(paths, (candidate, verified), strict=True):
        path.write_text(
            "".join(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                for record in records
            ),
            encoding="utf-8",
        )
    expected = build_decision_impact_report(
        candidate,
        verified,
        reconciliation_passed=True,
        verification_seconds=2.0,
        candidate_storage_bytes=10,
        verification_storage_bytes=20,
    )

    actual = build_decision_impact_report_from_ndjson(
        paths[0],
        paths[1],
        candidate_record_count=len(candidate),
        verified_record_count=len(verified),
        scratch_directory=tmp_path,
        reconciliation_passed=True,
        verification_seconds=2.0,
        candidate_storage_bytes=10,
        verification_storage_bytes=20,
    )

    assert actual == expected


def test_performance_summary_requires_real_measurements() -> None:
    assert summarize([3.0, 1.0, 2.0]) == {
        "median": 2.0,
        "minimum": 1.0,
        "maximum": 3.0,
    }
    with pytest.raises(ValueError):
        summarize([])
    assert Decimal("0.1") + Decimal("0.2") == Decimal("0.3")

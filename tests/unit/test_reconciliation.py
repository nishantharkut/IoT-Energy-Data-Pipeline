from __future__ import annotations

import json
from pathlib import Path

import pytest

from jobs.hadoop.reconcile import (
    reconcile,
    reconcile_or_raise,
    reconcile_sorted_ndjson,
)


def _record(**changes) -> dict:
    base = {
        "group_key": "meter-a|kWh|2",
        "meter_id": "meter-a",
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "decimal_scale": 2,
        "count": 2,
        "sum_scaled": 200,
        "min_event_time": "2024-01-01T00:00:00Z",
        "max_event_time": "2024-01-01T01:00:00Z",
        "quality_flag_count": 0,
    }
    return {**base, **changes}


def test_reconciliation_is_order_independent_and_exact() -> None:
    records = [_record(), _record(group_key="meter-b|kWh|2", meter_id="meter-b")]

    assert reconcile(records, reversed(records)) == []


def test_reconciliation_reports_group_and_field_names_precisely() -> None:
    errors = reconcile(
        [_record(), _record(group_key="missing|kWh|2", meter_id="missing")],
        [_record(sum_scaled=201), _record(group_key="extra|kWh|2", meter_id="extra")],
    )

    assert "meter-a|kWh|2.sum_scaled: expected 200, got 201" in errors
    assert "missing group: missing|kWh|2" in errors
    assert "unexpected group: extra|kWh|2" in errors


def test_reconciliation_rejects_duplicate_group_keys() -> None:
    errors = reconcile([_record(), _record()], [_record()])
    assert errors == ["duplicate expected group_key: meter-a|kWh|2"]

    with pytest.raises(ValueError, match="reconciliation failed"):
        reconcile_or_raise([_record()], [_record(count=3)])


def _write(path: Path, records: list[dict]) -> None:
    path.write_text(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            for record in records
        ),
        encoding="utf-8",
    )


def test_sorted_file_reconciliation_compares_exact_fields_without_indexing(
    tmp_path: Path,
) -> None:
    expected = tmp_path / "expected.ndjson"
    actual = tmp_path / "actual.ndjson"
    records = [_record(), _record(group_key="meter-b|kWh|2", meter_id="meter-b")]
    _write(expected, records)
    _write(actual, records)

    comparison = reconcile_sorted_ndjson(
        expected,
        actual,
        key_field="group_key",
        record_label="group",
        expected_count=2,
        actual_count=2,
    )

    assert comparison["errors"] == []
    assert comparison["expected_record_count"] == 2
    assert "sum_scaled" in comparison["exact_fields"]


def test_sorted_file_reconciliation_reports_missing_and_changed_records(
    tmp_path: Path,
) -> None:
    expected = tmp_path / "expected.ndjson"
    actual = tmp_path / "actual.ndjson"
    _write(
        expected,
        [_record(), _record(group_key="meter-b|kWh|2", meter_id="meter-b")],
    )
    _write(actual, [_record(sum_scaled=201)])

    comparison = reconcile_sorted_ndjson(
        expected,
        actual,
        key_field="group_key",
        record_label="group",
        expected_count=2,
        actual_count=1,
    )

    assert "meter-a|kWh|2.sum_scaled: expected 200, got 201" in comparison["errors"]
    assert "missing group: meter-b|kWh|2" in comparison["errors"]

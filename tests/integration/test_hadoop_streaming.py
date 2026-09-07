from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


def _event(value: int, time: str) -> dict:
    return {
        "meter_id": "meter-a",
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "decimal_scale": 2,
        "scaled_value": value,
        "event_time_utc": time,
        "quality_flags": [],
    }


def _canonical_event() -> dict:
    locator = "fixture-v1|clean|readings.xlsx|Readings|2"
    return {
        "schema_version": "canonical-event-v1",
        "dataset_version": "fixture-v1",
        "source_variant": "clean",
        "source_event_id": hashlib.sha256(locator.encode()).hexdigest(),
        "source_relative_path": "readings.xlsx",
        "workbook_sheet": "Readings",
        "source_row_number": 2,
        "meter_id": "meter-a",
        "brick_entity_id": None,
        "brick_building_ids": [],
        "brick_zone_ids": [],
        "brick_metered_entities": [],
        "brick_unit_uri": None,
        "brick_usage_type": None,
        "original_timestamp_text": "2024-01-01T00:00:00",
        "source_timezone": "Asia/Hong_Kong",
        "event_time_utc": "2023-12-31T16:00:00Z",
        "original_numeric_text": "10.00",
        "scaled_value": 1000,
        "decimal_scale": 2,
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "quality_flags": ["brick_unmatched"],
    }


def test_mapper_and_reducer_run_as_independent_streaming_processes(
    tmp_path: Path,
) -> None:
    source = "".join(
        json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
        for event in (
            _event(250, "2024-01-01T01:00:00Z"),
            _event(-50, "2024-01-01T00:00:00Z"),
        )
    )
    mapper = subprocess.run(
        [sys.executable, "-m", "jobs.hadoop.mapper"],
        input=source,
        text=True,
        capture_output=True,
        check=True,
    )
    sorted_mapper = "\n".join(sorted(mapper.stdout.splitlines())) + "\n"
    reducer = subprocess.run(
        [sys.executable, "-m", "jobs.hadoop.reducer"],
        input=sorted_mapper,
        text=True,
        capture_output=True,
        check=True,
    )

    result = json.loads(reducer.stdout)
    assert result["count"] == 2
    assert result["sum_scaled"] == 200
    assert result["min_event_time"] == "2024-01-01T00:00:00Z"


def test_mapper_process_fails_instead_of_skipping_invalid_source() -> None:
    mapper = subprocess.run(
        [sys.executable, "-m", "jobs.hadoop.mapper"],
        input='{"scaled_value":"bad"}\n',
        text=True,
        capture_output=True,
        check=False,
    )

    assert mapper.returncode != 0
    assert "line 1" in mapper.stderr


def test_exact_record_oracle_processes_sort_and_preserve_canonical_fields() -> None:
    event = _canonical_event()
    mapper = subprocess.run(
        [sys.executable, "-m", "jobs.hadoop.record_mapper"],
        input=json.dumps(event) + "\n",
        text=True,
        capture_output=True,
        check=True,
    )
    reducer = subprocess.run(
        [sys.executable, "-m", "jobs.hadoop.record_reducer"],
        input=mapper.stdout,
        text=True,
        capture_output=True,
        check=True,
    )

    assert json.loads(reducer.stdout) == event

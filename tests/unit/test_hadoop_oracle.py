from __future__ import annotations

import pytest

from jobs.hadoop.mapper import map_record
from jobs.hadoop.reducer import reduce_records


def _event(value: int, time: str, flags: list[str] | None = None) -> dict:
    return {
        "meter_id": "meter-a",
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "decimal_scale": 2,
        "scaled_value": value,
        "event_time_utc": time,
        "quality_flags": flags or [],
    }


def test_independent_mapper_and_reducer_match_gold_exact_fields() -> None:
    mapped = [
        map_record(_event(250, "2024-01-01T01:00:00Z", ["flag-a"])),
        map_record(_event(-50, "2024-01-01T00:00:00Z")),
    ]
    assert mapped[0][0] == "meter-a|kWh|2"

    reduced = reduce_records(mapped[0][0], [item[1] for item in mapped])

    assert reduced == {
        "group_key": "meter-a|kWh|2",
        "meter_id": "meter-a",
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "decimal_scale": 2,
        "count": 2,
        "sum_scaled": 200,
        "min_event_time": "2024-01-01T00:00:00Z",
        "max_event_time": "2024-01-01T01:00:00Z",
        "quality_flag_count": 1,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"scaled_value": "1"},
        {"scaled_value": True},
        {"quality_flags": "flag"},
        {"event_time_utc": ""},
    ],
)
def test_mapper_fails_closed_on_contract_violation(change: dict) -> None:
    with pytest.raises((TypeError, ValueError)):
        map_record({**_event(1, "2024-01-01T00:00:00Z"), **change})


def test_reducer_rejects_mixed_group_dimensions() -> None:
    _, first = map_record(_event(1, "2024-01-01T00:00:00Z"))
    _, second = map_record(
        {**_event(2, "2024-01-01T01:00:00Z"), "measurement_kind": "power"}
    )

    with pytest.raises(ValueError, match="mixed measurement_kind"):
        reduce_records("meter-a|kWh|2", [first, second])

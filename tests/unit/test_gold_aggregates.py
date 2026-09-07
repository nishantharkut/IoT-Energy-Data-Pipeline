from __future__ import annotations

import pytest

from jobs.spark.aggregates import aggregate


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


def test_gold_aggregate_uses_exact_signed_values_and_bounds() -> None:
    result = aggregate(
        [
            _event(250, "2024-01-01T01:00:00Z", ["flag-a"]),
            _event(-50, "2024-01-01T00:00:00Z"),
        ]
    )

    assert result == [
        {
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
    ]


def test_group_rejects_mixed_measurement_semantics() -> None:
    with pytest.raises(ValueError, match="mixed measurement kinds"):
        aggregate(
            [
                _event(1, "2024-01-01T00:00:00Z"),
                {
                    **_event(2, "2024-01-01T01:00:00Z"),
                    "measurement_kind": "interval_energy",
                },
            ]
        )

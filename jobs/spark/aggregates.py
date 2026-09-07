"""Exact Gold aggregate contract shared by local and Spark batch execution."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def aggregate(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        key = (
            str(event.get("meter_id")),
            str(event.get("unit", "UNKNOWN")),
            int(event.get("decimal_scale", 0)),
        )
        groups[key].append(event)

    output: list[dict[str, Any]] = []
    for (meter_id, unit, scale), values in groups.items():
        measurement_kinds = {
            str(value.get("measurement_kind", "UNKNOWN")) for value in values
        }
        if len(measurement_kinds) != 1:
            raise ValueError(f"mixed measurement kinds for {meter_id}|{unit}|{scale}")
        output.append(
            {
                "group_key": f"{meter_id}|{unit}|{scale}",
                "meter_id": meter_id,
                "measurement_kind": next(iter(measurement_kinds)),
                "unit": unit,
                "decimal_scale": scale,
                "count": len(values),
                "sum_scaled": sum(int(value["scaled_value"]) for value in values),
                "min_event_time": min(value["event_time_utc"] for value in values),
                "max_event_time": max(value["event_time_utc"] for value in values),
                "quality_flag_count": sum(
                    len(value.get("quality_flags", [])) for value in values
                ),
            }
        )
    return sorted(output, key=lambda record: record["group_key"])

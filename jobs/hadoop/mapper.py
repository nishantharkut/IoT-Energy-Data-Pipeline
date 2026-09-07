"""Independent Hadoop Streaming mapper over canonical NDJSON."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any

MIN_INT64, MAX_INT64 = -(2**63), 2**63 - 1


def _required_text(record: dict[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    if "\t" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{field} contains a streaming delimiter")
    return value


def map_record(record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Map one canonical source event without importing Spark implementation."""

    if not isinstance(record, dict):
        raise TypeError("canonical record must be an object")
    meter_id = _required_text(record, "meter_id")
    measurement_kind = _required_text(record, "measurement_kind")
    unit = _required_text(record, "unit")
    scale = record.get("decimal_scale")
    if isinstance(scale, bool) or not isinstance(scale, int) or scale < 0:
        raise ValueError("decimal_scale must be a non-negative integer")
    scaled = record.get("scaled_value")
    if isinstance(scaled, bool) or not isinstance(scaled, int):
        raise TypeError("scaled_value must be an integer")
    if not MIN_INT64 <= scaled <= MAX_INT64:
        raise ValueError("scaled_value exceeds signed 64-bit bounds")
    event_time = _required_text(record, "event_time_utc")
    try:
        parsed = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time_utc is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError("event_time_utc must be timezone-aware")
    quality = record.get("quality_flags")
    if not isinstance(quality, list) or any(
        not isinstance(flag, str) for flag in quality
    ):
        raise TypeError("quality_flags must be an array of strings")
    key = f"{meter_id}|{unit}|{scale}"
    return key, {
        "meter_id": meter_id,
        "measurement_kind": measurement_kind,
        "unit": unit,
        "decimal_scale": scale,
        "count": 1,
        "sum_scaled": scaled,
        "min_event_time": event_time,
        "max_event_time": event_time,
        "quality_flag_count": len(quality),
    }


def main() -> int:
    for line_number, line in enumerate(sys.stdin, start=1):
        try:
            value = json.loads(line)
            key, mapped = map_record(value)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            print(f"mapper line {line_number}: {exc}", file=sys.stderr)
            return 2
        print(key + "\t" + json.dumps(mapped, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

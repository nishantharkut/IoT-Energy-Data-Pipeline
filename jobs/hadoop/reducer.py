"""Independent Hadoop Streaming reducer for exact source aggregates."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass
class _Accumulator:
    meter_id: str
    measurement_kind: str
    unit: str
    decimal_scale: int
    count: int = 0
    sum_scaled: int = 0
    min_event_time: str | None = None
    max_event_time: str | None = None
    quality_flag_count: int = 0

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "_Accumulator":
        return cls(
            meter_id=str(record["meter_id"]),
            measurement_kind=str(record["measurement_kind"]),
            unit=str(record["unit"]),
            decimal_scale=int(record["decimal_scale"]),
        )

    def add(self, record: dict[str, Any]) -> None:
        dimensions = {
            "meter_id": self.meter_id,
            "measurement_kind": self.measurement_kind,
            "unit": self.unit,
            "decimal_scale": self.decimal_scale,
        }
        for field, expected in dimensions.items():
            if record.get(field) != expected:
                raise ValueError(f"mixed {field} values in one group")
        self.count += int(record["count"])
        self.sum_scaled += int(record["sum_scaled"])
        minimum = str(record["min_event_time"])
        maximum = str(record["max_event_time"])
        self.min_event_time = (
            minimum
            if self.min_event_time is None
            else min(self.min_event_time, minimum)
        )
        self.max_event_time = (
            maximum
            if self.max_event_time is None
            else max(self.max_event_time, maximum)
        )
        self.quality_flag_count += int(record["quality_flag_count"])

    def result(self, key: str) -> dict[str, Any]:
        if self.count < 1 or self.min_event_time is None or self.max_event_time is None:
            raise ValueError("cannot reduce an empty group")
        return {
            "group_key": key,
            "meter_id": self.meter_id,
            "measurement_kind": self.measurement_kind,
            "unit": self.unit,
            "decimal_scale": self.decimal_scale,
            "count": self.count,
            "sum_scaled": self.sum_scaled,
            "min_event_time": self.min_event_time,
            "max_event_time": self.max_event_time,
            "quality_flag_count": self.quality_flag_count,
        }


def reduce_records(key: str, records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    iterator = iter(records)
    try:
        first = next(iterator)
    except StopIteration as exc:
        raise ValueError("cannot reduce an empty group") from exc
    accumulator = _Accumulator.from_record(first)
    accumulator.add(first)
    for record in iterator:
        accumulator.add(record)
    return accumulator.result(key)


def main() -> int:
    current_key: str | None = None
    accumulator: _Accumulator | None = None
    try:
        for line_number, line in enumerate(sys.stdin, start=1):
            if "\t" not in line:
                raise ValueError(f"reducer line {line_number}: missing key separator")
            key, payload = line.rstrip("\n").split("\t", 1)
            record = json.loads(payload)
            if current_key is not None and key < current_key:
                raise ValueError("mapper input is not sorted by key")
            if current_key is not None and key != current_key:
                assert accumulator is not None
                print(
                    json.dumps(
                        accumulator.result(current_key),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                accumulator = None
            if accumulator is None:
                accumulator = _Accumulator.from_record(record)
                current_key = key
            accumulator.add(record)
        if current_key is not None:
            assert accumulator is not None
            print(
                json.dumps(
                    accumulator.result(current_key),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
    except (
        AssertionError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        print(f"reducer: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Independent uniqueness reducer for sorted canonical source records."""

from __future__ import annotations

import json
import sys
from typing import Any, Iterable


def reduce_source_records(
    key: str, records: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    values = list(records)
    if not values:
        raise ValueError("cannot reduce an empty source identity")
    if len(values) != 1:
        raise ValueError(f"duplicate source_event_id: {key}")
    record = values[0]
    if record.get("source_event_id") != key:
        raise ValueError("reducer key differs from source_event_id")
    return record


def main() -> int:
    current_key: str | None = None
    current_records: list[dict[str, Any]] = []
    try:
        for line_number, line in enumerate(sys.stdin, start=1):
            if "\t" not in line:
                raise ValueError(
                    f"record reducer line {line_number}: missing key separator"
                )
            key, payload = line.rstrip("\n").split("\t", 1)
            record = json.loads(payload)
            if not isinstance(record, dict):
                raise TypeError("record reducer payload must be an object")
            if current_key is not None and key < current_key:
                raise ValueError("record mapper input is not sorted by key")
            if current_key is not None and key != current_key:
                print(
                    json.dumps(
                        reduce_source_records(current_key, current_records),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                current_records = []
            current_key = key
            current_records.append(record)
        if current_key is not None:
            print(
                json.dumps(
                    reduce_source_records(current_key, current_records),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"record reducer: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

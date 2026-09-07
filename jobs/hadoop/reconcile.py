"""Field-level reconciliation between Spark Gold and the Hadoop oracle."""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


def _index(
    records: Iterable[dict[str, Any]], label: str, key_field: str
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    result: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for record in records:
        key = record.get(key_field)
        if not isinstance(key, str) or not key:
            errors.append(f"{label} record missing {key_field}")
            continue
        if key in result:
            errors.append(f"duplicate {label} {key_field}: {key}")
            continue
        result[key] = record
    return result, errors


def reconcile(
    expected: Iterable[dict[str, Any]], actual: Iterable[dict[str, Any]]
) -> list[str]:
    left, left_errors = _index(expected, "expected", "group_key")
    right, right_errors = _index(actual, "actual", "group_key")
    if left_errors or right_errors:
        return left_errors + right_errors
    errors: list[str] = []
    for key in sorted(set(left) - set(right)):
        errors.append(f"missing group: {key}")
    for key in sorted(set(right) - set(left)):
        errors.append(f"unexpected group: {key}")
    for key in sorted(set(left) & set(right)):
        expected_record = left[key]
        actual_record = right[key]
        for field in sorted(set(expected_record) | set(actual_record)):
            if expected_record.get(field) != actual_record.get(field):
                errors.append(
                    f"{key}.{field}: expected {expected_record.get(field)}, "
                    f"got {actual_record.get(field)}"
                )
    return errors


def reconcile_source_records(
    expected: Iterable[dict[str, Any]], actual: Iterable[dict[str, Any]]
) -> list[str]:
    """Compare full source records by identity and every declared field."""

    left, left_errors = _index(expected, "expected", "source_event_id")
    right, right_errors = _index(actual, "actual", "source_event_id")
    if left_errors or right_errors:
        return left_errors + right_errors
    errors: list[str] = []
    for key in sorted(set(left) - set(right)):
        errors.append(f"missing source record: {key}")
    for key in sorted(set(right) - set(left)):
        errors.append(f"unexpected source record: {key}")
    for key in sorted(set(left) & set(right)):
        expected_record = left[key]
        actual_record = right[key]
        for field in sorted(set(expected_record) | set(actual_record)):
            if expected_record.get(field) != actual_record.get(field):
                errors.append(
                    f"{key}.{field}: expected {expected_record.get(field)}, "
                    f"got {actual_record.get(field)}"
                )
    return errors


def reconcile_sorted_ndjson(
    expected_path: Path,
    actual_path: Path,
    *,
    key_field: str,
    record_label: str,
    expected_count: int,
    actual_count: int,
    maximum_reported_errors: int = 1000,
) -> dict[str, Any]:
    """Merge-compare two sorted NDJSON files with constant record memory."""

    if (
        isinstance(expected_count, bool)
        or not isinstance(expected_count, int)
        or expected_count < 0
        or isinstance(actual_count, bool)
        or not isinstance(actual_count, int)
        or actual_count < 0
        or isinstance(maximum_reported_errors, bool)
        or not isinstance(maximum_reported_errors, int)
        or maximum_reported_errors < 1
    ):
        raise ValueError("sorted reconciliation counts are invalid")
    exact_fields: set[str] = set()
    counts = {"expected": 0, "actual": 0}
    previous_keys: dict[str, str | None] = {"expected": None, "actual": None}

    def records(path: Path, side: str) -> Iterator[tuple[str, dict[str, Any]]]:
        try:
            stream = path.open("r", encoding="utf-8", newline="")
        except (OSError, UnicodeError) as exc:
            raise ValueError(f"cannot read {side} reconciliation records") from exc
        with stream:
            for line_number, line in enumerate(stream, start=1):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"{side} reconciliation line {line_number} is invalid JSON"
                    ) from exc
                if not isinstance(record, dict):
                    raise ValueError(
                        f"{side} reconciliation line {line_number} is not an object"
                    )
                key = record.get(key_field)
                if not isinstance(key, str) or not key:
                    raise ValueError(
                        f"{side} reconciliation line {line_number} has no {key_field}"
                    )
                previous = previous_keys[side]
                if previous is not None and key <= previous:
                    raise ValueError(
                        f"{side} reconciliation keys are duplicate or unsorted"
                    )
                previous_keys[side] = key
                counts[side] += 1
                exact_fields.update(record)
                yield key, record

    errors: list[str] = []
    total_errors = 0

    def add_error(message: str) -> None:
        nonlocal total_errors
        total_errors += 1
        if len(errors) < maximum_reported_errors:
            errors.append(message)

    expected_iterator = iter(records(expected_path, "expected"))
    actual_iterator = iter(records(actual_path, "actual"))
    expected_item = next(expected_iterator, None)
    actual_item = next(actual_iterator, None)
    while expected_item is not None or actual_item is not None:
        if expected_item is None:
            assert actual_item is not None
            add_error(f"unexpected {record_label}: {actual_item[0]}")
            actual_item = next(actual_iterator, None)
            continue
        if actual_item is None:
            add_error(f"missing {record_label}: {expected_item[0]}")
            expected_item = next(expected_iterator, None)
            continue
        if expected_item[0] < actual_item[0]:
            add_error(f"missing {record_label}: {expected_item[0]}")
            expected_item = next(expected_iterator, None)
            continue
        if actual_item[0] < expected_item[0]:
            add_error(f"unexpected {record_label}: {actual_item[0]}")
            actual_item = next(actual_iterator, None)
            continue
        key = expected_item[0]
        expected_record = expected_item[1]
        actual_record = actual_item[1]
        for field in sorted(set(expected_record) | set(actual_record)):
            if expected_record.get(field) != actual_record.get(field):
                add_error(
                    f"{key}.{field}: expected {expected_record.get(field)}, "
                    f"got {actual_record.get(field)}"
                )
        expected_item = next(expected_iterator, None)
        actual_item = next(actual_iterator, None)

    observed_expected = counts["expected"]
    observed_actual = counts["actual"]
    if observed_expected != expected_count:
        raise ValueError("expected reconciliation artifact record count mismatch")
    if observed_actual != actual_count:
        raise ValueError("actual reconciliation artifact record count mismatch")
    if total_errors > len(errors):
        errors.append(
            f"additional reconciliation errors omitted: {total_errors - len(errors)}"
        )
    return {
        "expected_record_count": observed_expected,
        "actual_record_count": observed_actual,
        "exact_fields": sorted(exact_fields),
        "errors": errors,
    }


def reconcile_or_raise(
    expected: Iterable[dict[str, Any]], actual: Iterable[dict[str, Any]]
) -> None:
    errors = reconcile(expected, actual)
    if errors:
        raise ValueError("reconciliation failed: " + "; ".join(errors))

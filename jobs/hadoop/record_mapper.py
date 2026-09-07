"""Independent exact-record validator and mapper for canonical NDJSON."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

MIN_INT64, MAX_INT64 = -(2**63), 2**63 - 1

REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "dataset_version",
        "source_variant",
        "source_event_id",
        "source_relative_path",
        "workbook_sheet",
        "source_row_number",
        "meter_id",
        "brick_entity_id",
        "brick_building_ids",
        "brick_zone_ids",
        "brick_metered_entities",
        "brick_unit_uri",
        "brick_usage_type",
        "original_timestamp_text",
        "source_timezone",
        "event_time_utc",
        "original_numeric_text",
        "scaled_value",
        "decimal_scale",
        "measurement_kind",
        "unit",
        "quality_flags",
    }
)


def _required_text(record: dict[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _optional_text(record: dict[str, Any], field: str) -> str | None:
    value = record.get(field)
    if value is not None and (not isinstance(value, str) or not value):
        raise ValueError(f"{field} must be null or a non-empty string")
    return value


def _text_list(record: dict[str, Any], field: str, *, sorted_: bool) -> list[str]:
    value = record.get(field)
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise TypeError(f"{field} must be an array of non-empty strings")
    if len(value) != len(set(value)):
        raise ValueError(f"{field} values must be unique")
    if sorted_ and value != sorted(value):
        raise ValueError(f"{field} values must be sorted")
    return value


def _validate_metered_entities(record: dict[str, Any]) -> None:
    values = record.get("brick_metered_entities")
    if not isinstance(values, list):
        raise TypeError("brick_metered_entities must be an array")
    entity_ids: list[str] = []
    for value in values:
        if not isinstance(value, dict) or set(value) != {"entity_id", "type_uris"}:
            raise ValueError("metered entity canonical field set is invalid")
        entity_ids.append(_required_text(value, "entity_id"))
        _text_list(value, "type_uris", sorted_=True)
    if len(entity_ids) != len(set(entity_ids)):
        raise ValueError("metered entity IDs must be unique")
    if entity_ids != sorted(entity_ids):
        raise ValueError("metered entity IDs must be sorted")


def map_source_record(record: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Validate one full canonical record and recompute its structural identity."""

    if not isinstance(record, dict):
        raise TypeError("canonical record must be an object")
    if set(record) != REQUIRED_FIELDS:
        raise ValueError("canonical field set does not match canonical-event-v1")
    if record.get("schema_version") != "canonical-event-v1":
        raise ValueError("unsupported canonical schema_version")
    source_variant = _required_text(record, "source_variant")
    if source_variant not in {"raw", "clean"}:
        raise ValueError("source_variant must be raw or clean")

    locator_fields = (
        _required_text(record, "dataset_version"),
        source_variant,
        _required_text(record, "source_relative_path"),
        _required_text(record, "workbook_sheet"),
    )
    if any("|" in value for value in locator_fields):
        raise ValueError("source locator fields cannot contain '|'")
    source_row = record.get("source_row_number")
    if (
        isinstance(source_row, bool)
        or not isinstance(source_row, int)
        or source_row < 1
    ):
        raise ValueError("source_row_number must be a positive integer")
    serialized = "|".join((*locator_fields, str(source_row)))
    expected_source_id = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    source_id = _required_text(record, "source_event_id")
    if source_id != expected_source_id:
        raise ValueError("source_event_id does not match the source locator")

    for field in (
        "meter_id",
        "original_timestamp_text",
        "source_timezone",
        "event_time_utc",
        "original_numeric_text",
        "measurement_kind",
        "unit",
    ):
        _required_text(record, field)
    brick_entity = _optional_text(record, "brick_entity_id")
    brick_unit = _optional_text(record, "brick_unit_uri")
    brick_usage = _optional_text(record, "brick_usage_type")
    buildings = _text_list(record, "brick_building_ids", sorted_=True)
    zones = _text_list(record, "brick_zone_ids", sorted_=True)
    _validate_metered_entities(record)
    flags = _text_list(record, "quality_flags", sorted_=True)
    if brick_entity is None and (
        buildings
        or zones
        or record["brick_metered_entities"]
        or brick_unit is not None
        or brick_usage is not None
    ):
        raise ValueError("unmatched Brick entity has non-empty enrichment")
    if brick_entity is None and "brick_unmatched" not in flags:
        raise ValueError("unmatched Brick entity lacks brick_unmatched quality flag")

    event_time = _required_text(record, "event_time_utc")
    try:
        parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("event_time_utc is invalid") from exc
    if parsed_time.tzinfo is None:
        raise ValueError("event_time_utc must be timezone-aware")

    scale = record.get("decimal_scale")
    if isinstance(scale, bool) or not isinstance(scale, int) or scale < 0:
        raise ValueError("decimal_scale must be a non-negative integer")
    scaled = record.get("scaled_value")
    if isinstance(scaled, bool) or not isinstance(scaled, int):
        raise TypeError("scaled_value must be an integer")
    if not MIN_INT64 <= scaled <= MAX_INT64:
        raise ValueError("scaled_value exceeds signed 64-bit bounds")
    try:
        reconstructed = Decimal(record["original_numeric_text"]) * (
            Decimal(10) ** scale
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("original_numeric_text is invalid") from exc
    if (
        not reconstructed.is_finite()
        or reconstructed != reconstructed.to_integral_value()
        or int(reconstructed) != scaled
    ):
        raise ValueError("scaled numeric representation does not match source text")
    return source_id, copy.deepcopy(record)


def main() -> int:
    for line_number, line in enumerate(sys.stdin, start=1):
        try:
            record = json.loads(line)
            key, mapped = map_source_record(record)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            print(f"record mapper line {line_number}: {exc}", file=sys.stderr)
            return 2
        print(key + "\t" + json.dumps(mapped, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

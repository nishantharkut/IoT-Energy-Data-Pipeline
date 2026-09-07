"""Shared Spark-side delivery validation without Hadoop dependencies."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

MIN_INT64, MAX_INT64 = -(2**63), 2**63 - 1

QUARANTINE_CATEGORIES = (
    "invalid_json",
    "schema_mismatch",
    "unknown_source_event_id",
    "invalid_timestamp",
    "invalid_numeric_value",
    "numeric_overflow",
    "missing_required_identity",
    "unit_or_scale_contract_violation",
    "run_manifest_mismatch",
)

REQUIRED_EVENT_FIELDS = frozenset(
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


def _header_dict(
    headers: Mapping[str, str] | Iterable[tuple[str, str] | Mapping[str, str]] | None,
) -> dict[str, str]:
    if headers is None:
        return {}
    if isinstance(headers, Mapping):
        return {str(key): str(value) for key, value in headers.items()}
    result: dict[str, str] = {}
    for header in headers:
        if isinstance(header, Mapping):
            result[str(header["key"])] = str(header["value"])
        else:
            key, value = header
            result[str(key)] = str(value)
    return result


def archive_kafka_record(
    *,
    topic: str,
    key: str | None,
    value: str,
    headers: Iterable[tuple[str, str]],
    partition: int,
    offset: int,
    kafka_timestamp: str,
    ingestion_timestamp: str,
) -> dict[str, Any]:
    """Project a Kafka record into the append-only Bronze contract."""

    return {
        "topic": topic,
        "key": key,
        "value": value,
        "headers": [
            {"key": str(header_key), "value": str(header_value)}
            for header_key, header_value in headers
        ],
        "partition": partition,
        "offset": offset,
        "kafka_timestamp": kafka_timestamp,
        "ingestion_timestamp": ingestion_timestamp,
    }


def _numeric_category(
    record: dict[str, Any], *, require_original_text: bool
) -> str | None:
    scaled = record.get("scaled_value")
    scale = record.get("decimal_scale")
    if isinstance(scaled, bool) or not isinstance(scaled, int):
        return "invalid_numeric_value"
    if scaled < MIN_INT64 or scaled > MAX_INT64:
        return "numeric_overflow"
    if isinstance(scale, bool) or not isinstance(scale, int) or scale < 0:
        return "invalid_numeric_value"
    if not require_original_text:
        return None
    original = record.get("original_numeric_text")
    if not isinstance(original, str):
        return "invalid_numeric_value"
    try:
        reconstructed = Decimal(original.strip()) * (Decimal(10) ** scale)
    except (InvalidOperation, ValueError):
        return "invalid_numeric_value"
    if (
        not reconstructed.is_finite()
        or reconstructed != reconstructed.to_integral_value()
        or int(reconstructed) != scaled
    ):
        return "invalid_numeric_value"
    return None


def _canonical_payload(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def parse_delivery(
    raw: str,
    known_events: set[str] | Mapping[str, dict[str, Any]],
    manifest_hash: str | None = None,
    headers: Mapping[str, str]
    | Iterable[tuple[str, str] | Mapping[str, str]]
    | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate one data delivery and return one finite quarantine category."""

    try:
        record = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None, "invalid_json"
    if not isinstance(record, dict):
        return None, "schema_mismatch"
    if not record.get("source_event_id") or not record.get("meter_id"):
        return None, "missing_required_identity"
    if manifest_hash is not None:
        header_values = _header_dict(headers)
        if header_values.get("replay_manifest_hash") != manifest_hash:
            return None, "run_manifest_mismatch"
        if record.get("schema_version") != "canonical-event-v1":
            return None, "schema_mismatch"
        if not REQUIRED_EVENT_FIELDS <= set(record):
            return None, "schema_mismatch"
    source_id = str(record["source_event_id"])
    if source_id not in known_events:
        return None, "unknown_source_event_id"
    event_time = record.get("event_time_utc")
    if not isinstance(event_time, str):
        return None, "invalid_timestamp"
    try:
        parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except ValueError:
        return None, "invalid_timestamp"
    if parsed_time.tzinfo is None:
        return None, "invalid_timestamp"
    numeric_category = _numeric_category(
        record, require_original_text=manifest_hash is not None
    )
    if numeric_category is not None:
        return None, numeric_category
    if isinstance(known_events, Mapping):
        expected = known_events[source_id]
        if record.get("unit") != expected.get("unit") or record.get(
            "decimal_scale"
        ) != expected.get("decimal_scale"):
            return None, "unit_or_scale_contract_violation"
        if record != expected:
            return None, "schema_mismatch"
    header_values = _header_dict(headers)
    declared_payload_hash = header_values.get("payload_sha256")
    if (
        declared_payload_hash is not None
        and declared_payload_hash != hashlib.sha256(raw.encode("utf-8")).hexdigest()
    ):
        return None, "schema_mismatch"
    return record, None


def layer_counts(
    deliveries: list[dict[str, Any]],
    quarantine: list[dict[str, Any]],
    silver: list[dict[str, Any]],
    *,
    duplicates: int = 0,
    late_valid: int = 0,
) -> dict[str, int]:
    return {
        "bronze_deliveries": len(deliveries),
        "quarantine": len(quarantine),
        "silver": len(silver),
        "parse_valid": len(deliveries) - len(quarantine),
        "duplicate_valid": duplicates,
        "late_valid": late_valid,
    }


def canonical_spark_schema() -> Any:
    """Construct the exact PySpark schema lazily for optional installations."""

    from pyspark.sql.types import (
        ArrayType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    fields = []
    for name in (
        "schema_version",
        "dataset_version",
        "source_variant",
        "source_event_id",
        "source_relative_path",
        "workbook_sheet",
    ):
        fields.append(StructField(name, StringType(), False))
    fields.append(StructField("source_row_number", LongType(), False))
    fields.append(StructField("meter_id", StringType(), False))
    fields.append(StructField("brick_entity_id", StringType(), True))
    fields.append(
        StructField("brick_building_ids", ArrayType(StringType(), False), False)
    )
    fields.append(StructField("brick_zone_ids", ArrayType(StringType(), False), False))
    fields.append(
        StructField(
            "brick_metered_entities",
            ArrayType(
                StructType(
                    [
                        StructField("entity_id", StringType(), False),
                        StructField("type_uris", ArrayType(StringType(), False), False),
                    ]
                ),
                False,
            ),
            False,
        )
    )
    fields.append(StructField("brick_unit_uri", StringType(), True))
    fields.append(StructField("brick_usage_type", StringType(), True))
    for name in (
        "original_timestamp_text",
        "source_timezone",
        "event_time_utc",
        "original_numeric_text",
    ):
        fields.append(StructField(name, StringType(), False))
    fields.append(StructField("scaled_value", LongType(), False))
    fields.append(StructField("decimal_scale", LongType(), False))
    fields.append(StructField("measurement_kind", StringType(), False))
    fields.append(StructField("unit", StringType(), False))
    fields.append(StructField("quality_flags", ArrayType(StringType(), False), False))
    return StructType(fields)

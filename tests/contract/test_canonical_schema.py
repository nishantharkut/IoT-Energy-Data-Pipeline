from __future__ import annotations

import json
from pathlib import Path

from jobs.hadoop.record_mapper import REQUIRED_FIELDS as HADOOP_REQUIRED_FIELDS
from jobs.spark.common import REQUIRED_EVENT_FIELDS as SPARK_REQUIRED_FIELDS

REQUIRED_FIELDS = {
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


def test_canonical_schema_is_closed_and_requires_full_provenance() -> None:
    schema = json.loads(
        Path("schemas/canonical-event-v1.schema.json").read_text(encoding="utf-8")
    )

    assert set(schema["required"]) == REQUIRED_FIELDS
    assert HADOOP_REQUIRED_FIELDS == REQUIRED_FIELDS
    assert SPARK_REQUIRED_FIELDS == REQUIRED_FIELDS
    assert schema["additionalProperties"] is False


def test_canonical_schema_declares_exact_numeric_and_time_constraints() -> None:
    schema = json.loads(
        Path("schemas/canonical-event-v1.schema.json").read_text(encoding="utf-8")
    )
    properties = schema["properties"]

    assert properties["scaled_value"]["minimum"] == -(2**63)
    assert properties["scaled_value"]["maximum"] == 2**63 - 1
    assert properties["event_time_utc"]["format"] == "date-time"
    assert properties["source_timezone"]["minLength"] == 1
    assert properties["quality_flags"]["uniqueItems"] is True
    assert properties["brick_building_ids"]["uniqueItems"] is True
    assert properties["brick_zone_ids"]["uniqueItems"] is True
    metered_entity = properties["brick_metered_entities"]["items"]
    assert metered_entity["additionalProperties"] is False
    assert metered_entity["properties"]["type_uris"]["uniqueItems"] is True

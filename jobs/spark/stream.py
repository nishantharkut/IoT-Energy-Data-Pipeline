"""Spark Structured Streaming layers plus a deterministic fixture reference."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from iot_energy_pipeline.schedule import ReplaySchedule, max_event_time_lateness
from jobs.spark.common import canonical_spark_schema, layer_counts, parse_delivery


def assert_retaining_watermark(schedule: ReplaySchedule, watermark: timedelta) -> None:
    if watermark.total_seconds() < max_event_time_lateness(schedule):
        raise ValueError(
            "configured watermark does not retain the resolved schedule lateness"
        )


def process_deliveries(
    deliveries: list[dict[str, Any]],
    known_events: set[str] | Mapping[str, dict[str, Any]],
    watermark: timedelta | None = None,
    manifest_hash: str | None = None,
) -> dict[str, Any]:
    """Reference the exact layer accounting used by fast fixture tests."""

    bronze = list(deliveries)
    quarantine: list[dict[str, Any]] = []
    parsed: list[dict[str, Any]] = []
    for delivery in deliveries:
        raw_value = delivery.get("payload", delivery.get("value"))
        raw = raw_value if isinstance(raw_value, str) else ""
        event, category = parse_delivery(
            raw,
            known_events,
            manifest_hash,
            delivery.get("headers"),
        )
        if category:
            quarantine.append({**delivery, "category": category})
        elif event is not None:
            parsed.append({**delivery, "event": event})

    silver: list[dict[str, Any]] = []
    duplicate_valid: list[dict[str, Any]] = []
    late_valid: list[dict[str, Any]] = []
    seen: set[str] = set()
    latest: datetime | None = None
    for item in parsed:
        event_time = datetime.fromisoformat(
            item["event"]["event_time_utc"].replace("Z", "+00:00")
        )
        if (
            watermark is not None
            and latest is not None
            and event_time < latest - watermark
        ):
            late_valid.append(item)
            continue
        source_id = item["event"]["source_event_id"]
        if source_id in seen:
            duplicate_valid.append(item)
            continue
        seen.add(source_id)
        silver.append(item)
        if latest is None or event_time > latest:
            latest = event_time

    return {
        "bronze": bronze,
        "parsed": parsed,
        "silver": silver,
        "duplicate_valid": duplicate_valid,
        "late_valid": late_valid,
        "quarantine": quarantine,
        "counts": layer_counts(
            bronze,
            quarantine,
            silver,
            duplicates=len(duplicate_valid),
            late_valid=len(late_valid),
        ),
    }


def serialize_event(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True, separators=(",", ":"))


def _json_shape(raw: str) -> str | None:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return "invalid_json"
    return None if isinstance(value, dict) else "schema_mismatch"


def _numeric_shape(raw: str) -> str | None:
    try:
        value = json.loads(raw)
        scaled = value.get("scaled_value")
    except (json.JSONDecodeError, AttributeError, TypeError):
        return None
    if isinstance(scaled, bool) or not isinstance(scaled, int):
        return "invalid_numeric_value"
    if scaled < -(2**63) or scaled > 2**63 - 1:
        return "numeric_overflow"
    return None


def run_structured_stream(
    spark: Any,
    *,
    bootstrap_servers: str,
    topic: str,
    canonical_ndjson: str,
    output_root: str,
    checkpoint_root: str,
    replay_manifest_hash: str,
    watermark: str,
    max_offsets_per_trigger: int | None = None,
) -> None:
    """Run actual bounded Kafka Structured Streaming queries with ``availableNow``."""

    from pyspark.sql import functions as functions
    from pyspark.sql.types import StringType

    schema = canonical_spark_schema()
    if max_offsets_per_trigger is not None and max_offsets_per_trigger < 1:
        raise ValueError("max_offsets_per_trigger must be positive")
    source_builder = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("includeHeaders", "true")
    )
    if max_offsets_per_trigger is not None:
        source_builder = source_builder.option(
            "maxOffsetsPerTrigger", max_offsets_per_trigger
        )
    source = source_builder.load()
    raw = source.select(
        functions.col("topic"),
        functions.col("key").cast("string").alias("key"),
        functions.col("value").cast("string").alias("value"),
        functions.col("headers"),
        functions.col("partition"),
        functions.col("offset"),
        functions.col("timestamp").alias("kafka_timestamp"),
        functions.current_timestamp().alias("ingestion_timestamp"),
    ).withColumn(
        "header_map",
        functions.expr(
            "map_from_entries(transform(headers, x -> "
            "struct(x.key, cast(x.value as string))))"
        ),
    )
    bound_output = str(Path(output_root) / replay_manifest_hash)
    bound_checkpoint = str(Path(checkpoint_root) / replay_manifest_hash)
    terminals = raw.where(
        functions.element_at("header_map", functions.lit("record_type"))
        == functions.lit("terminal")
    )
    data = raw.where(
        functions.element_at("header_map", functions.lit("record_type"))
        == functions.lit("data")
    )
    bronze_query = (
        data.writeStream.format("parquet")
        .option("path", str(Path(bound_output) / "bronze"))
        .option("checkpointLocation", str(Path(bound_checkpoint) / "bronze"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    terminal_query = (
        terminals.writeStream.format("parquet")
        .option("path", str(Path(bound_output) / "terminals"))
        .option("checkpointLocation", str(Path(bound_checkpoint) / "terminals"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    parsed = data.withColumn("event", functions.from_json("value", schema))
    syntax_udf = functions.udf(_json_shape, StringType())
    numeric_udf = functions.udf(_numeric_shape, StringType())
    parsed = parsed.withColumn("syntax_category", syntax_udf("value")).withColumn(
        "numeric_category", numeric_udf("value")
    )

    canonical_raw = spark.read.text(canonical_ndjson).select(
        functions.col("value").alias("canonical_value"),
        functions.from_json("value", schema).alias("canonical_event"),
    )
    canonical = canonical_raw.select(
        functions.col("canonical_event.source_event_id").alias("known_source_event_id"),
        functions.col("canonical_event.unit").alias("expected_unit"),
        functions.col("canonical_event.decimal_scale").alias("expected_scale"),
        functions.sha2("canonical_value", 256).alias("expected_payload_sha256"),
    )
    joined = parsed.join(
        canonical,
        functions.col("event.source_event_id")
        == functions.col("known_source_event_id"),
        "left",
    )
    missing_fields = (
        functions.col("event.source_event_id").isNull()
        | functions.col("event.meter_id").isNull()
    )
    category = (
        functions.when(
            functions.col("syntax_category").isNotNull(),
            functions.col("syntax_category"),
        )
        .when(missing_fields, functions.lit("missing_required_identity"))
        .when(
            ~functions.element_at(
                "header_map", functions.lit("replay_manifest_hash")
            ).eqNullSafe(functions.lit(replay_manifest_hash)),
            functions.lit("run_manifest_mismatch"),
        )
        .when(
            functions.col("known_source_event_id").isNull(),
            functions.lit("unknown_source_event_id"),
        )
        .when(
            functions.to_timestamp("event.event_time_utc").isNull(),
            functions.lit("invalid_timestamp"),
        )
        .when(
            functions.col("numeric_category").isNotNull(),
            functions.col("numeric_category"),
        )
        .when(
            (~functions.col("event.unit").eqNullSafe(functions.col("expected_unit")))
            | (
                ~functions.col("event.decimal_scale").eqNullSafe(
                    functions.col("expected_scale")
                )
            ),
            functions.lit("unit_or_scale_contract_violation"),
        )
        .when(
            ~functions.sha2("value", 256).eqNullSafe(
                functions.col("expected_payload_sha256")
            ),
            functions.lit("schema_mismatch"),
        )
    )
    classified = joined.withColumn("quarantine_category", category)
    quarantine = classified.where(functions.col("quarantine_category").isNotNull())
    valid = classified.where(functions.col("quarantine_category").isNull()).select(
        "topic",
        "key",
        "value",
        "headers",
        "partition",
        "offset",
        "kafka_timestamp",
        "ingestion_timestamp",
        "event.*",
    )
    parsed_query = (
        valid.writeStream.format("parquet")
        .option("path", str(Path(bound_output) / "parsed"))
        .option("checkpointLocation", str(Path(bound_checkpoint) / "parsed"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    quarantine_query = (
        quarantine.writeStream.format("parquet")
        .option("path", str(Path(bound_output) / "quarantine"))
        .option("checkpointLocation", str(Path(bound_checkpoint) / "quarantine"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    silver = (
        valid.withColumn("event_time", functions.to_timestamp("event_time_utc"))
        .withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["source_event_id"])
    )
    silver_query = (
        silver.writeStream.format("parquet")
        .option("path", str(Path(bound_output) / "silver"))
        .option("checkpointLocation", str(Path(bound_checkpoint) / "silver"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    for query in (
        bronze_query,
        terminal_query,
        parsed_query,
        quarantine_query,
        silver_query,
    ):
        query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--canonical-ndjson", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--replay-manifest-hash", required=True)
    parser.add_argument("--watermark", required=True)
    parser.add_argument("--max-offsets-per-trigger", type=int)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("iot-energy-stream").getOrCreate()
    try:
        run_structured_stream(
            spark,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            canonical_ndjson=args.canonical_ndjson,
            output_root=args.output_root,
            checkpoint_root=args.checkpoint_root,
            replay_manifest_hash=args.replay_manifest_hash,
            watermark=args.watermark,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

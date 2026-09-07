"""Exact Spark batch Gold finalization over replay-complete Bronze Parquet."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from iot_energy_pipeline.replay import load_replay_manifest, write_json
from iot_energy_pipeline.streaming_replay import (
    load_streaming_replay_receipt,
    load_streaming_schedule_metadata,
)
from jobs.spark.common import canonical_spark_schema


def _load_plan(run_dir: Path) -> dict[str, Any]:
    try:
        plan = json.loads(
            (run_dir / "distributed-plan.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("cannot read distributed fixture plan") from exc
    if not isinstance(plan, dict) or plan.get("status") != "prepared":
        raise ValueError("distributed fixture plan is not prepared")
    return plan


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _require_empty(frame: Any, message: str) -> None:
    if frame.limit(1).count():
        raise ValueError(message)


def _require_unique(frame: Any, key: str, message: str) -> None:
    from pyspark.sql import functions as functions

    duplicates = frame.groupBy(key).count().where(functions.col("count") != 1)
    _require_empty(duplicates, message)


def _move_single_part(source: Path, destination: Path) -> None:
    parts = sorted(source.glob("part-*"))
    if len(parts) != 1:
        raise ValueError(f"expected exactly one Spark part in {source}")
    parts[0].replace(destination)
    shutil.rmtree(source)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_manifest(path: Path) -> dict[str, Any]:
    files = sorted(
        (item for item in path.rglob("*") if item.is_file()),
        key=lambda item: item.relative_to(path).as_posix(),
    )
    entries = [
        {
            "relative_path": item.relative_to(path).as_posix(),
            "byte_size": item.stat().st_size,
            "sha256": _file_sha256(item),
        }
        for item in files
    ]
    tree_payload = json.dumps(
        entries, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return {
        "directory": path.name,
        "file_count": len(entries),
        "byte_size": sum(entry["byte_size"] for entry in entries),
        "tree_sha256": hashlib.sha256(tree_payload).hexdigest(),
    }


def finalize_distributed_gold(
    spark: Any,
    *,
    run_dir: Path,
    layers_root: str,
) -> dict[str, Any]:
    """Validate all replay boundaries and write immutable exact Spark Gold."""

    started = time.perf_counter()
    from pyspark.sql import Window
    from pyspark.sql import functions as functions
    from pyspark.sql.types import LongType, MapType, StringType

    plan = _load_plan(run_dir)
    manifest_path = run_dir / str(plan["replay_manifest_path"])
    manifest = load_replay_manifest(manifest_path)
    _require(
        manifest.sha256() == plan.get("replay_manifest_hash"),
        "distributed plan replay manifest hash mismatch",
    )
    manifest_hash = manifest.sha256()
    _require(
        manifest.schedule_hash == plan.get("schedule_hash"),
        "distributed plan schedule hash mismatch",
    )
    gold_root = run_dir / "gold"
    if gold_root.exists():
        raise FileExistsError(f"immutable Gold output already exists: {gold_root}")

    schema = canonical_spark_schema()
    canonical_path = run_dir / str(plan["canonical_ndjson_path"])
    canonical_raw = spark.read.text(str(canonical_path)).select(
        functions.col("value").alias("canonical_payload"),
        functions.from_json("value", schema).alias("canonical_event"),
    )
    _require_empty(
        canonical_raw.where(functions.col("canonical_event").isNull()),
        "canonical snapshot contains invalid records",
    )
    canonical = canonical_raw.select(
        "canonical_payload",
        functions.sha2("canonical_payload", 256).alias("canonical_payload_sha256"),
        "canonical_event.*",
    )
    _require_unique(
        canonical,
        "source_event_id",
        "canonical snapshot contains duplicate source_event_id",
    )
    canonical_count = canonical.count()
    _require(
        canonical_count == manifest.requested_event_count,
        "canonical source count does not match replay manifest",
    )
    _require(
        _file_sha256(canonical_path) == manifest.snapshot_hash,
        "canonical snapshot byte hash mismatch",
    )

    schedule_path = run_dir / str(plan["schedule_path"])
    if plan.get("schedule_format") == "delivery-ndjson-v2":
        metadata_path = run_dir / str(plan.get("schedule_metadata_path"))
        _require(
            _file_sha256(metadata_path) == plan.get("schedule_metadata_sha256"),
            "schedule metadata hash does not match the distributed plan",
        )
        schedule_metadata = load_streaming_schedule_metadata(
            metadata_path, schedule_path=schedule_path
        )
        _require(
            schedule_metadata.run_id == manifest.replay_run_id
            and schedule_metadata.schedule_sha256 == manifest.schedule_hash
            and schedule_metadata.partition_count == manifest.partition_count
            and schedule_metadata.delivery_count == manifest.expected_delivery_count,
            "schedule metadata does not match replay manifest",
        )
        schedule = spark.read.json(str(schedule_path))
    else:
        schedule_document = spark.read.option("multiline", "true").json(
            str(schedule_path)
        )
        schedule_header = schedule_document.select(
            "run_id", "schedule_hash", "partition_count"
        ).head()
        _require(schedule_header is not None, "schedule artifact is empty")
        _require(
            schedule_header.run_id == manifest.replay_run_id
            and schedule_header.schedule_hash == manifest.schedule_hash
            and schedule_header.partition_count == manifest.partition_count,
            "schedule header does not match replay manifest",
        )
        schedule = schedule_document.select(
            functions.explode("deliveries").alias("delivery")
        ).select("delivery.*")
    _require(
        schedule.count() == manifest.expected_delivery_count,
        "schedule delivery count mismatch",
    )
    _require_unique(schedule, "delivery_id", "schedule delivery IDs are not unique")

    receipt_path = run_dir / "replay/replay-receipt.json"
    if plan.get("schedule_format") == "delivery-ndjson-v2":
        acknowledgement_path = run_dir / "replay/replay-acknowledgements.ndjson"
        receipt = load_streaming_replay_receipt(
            receipt_path, acknowledgement_path=acknowledgement_path
        )
        _require(
            receipt.get("replay_run_id") == manifest.replay_run_id
            and receipt.get("replay_manifest_hash") == manifest_hash
            and receipt.get("schedule_hash") == manifest.schedule_hash
            and receipt.get("expected_delivery_count")
            == manifest.expected_delivery_count
            and receipt.get("acknowledged_delivery_count")
            == manifest.expected_delivery_count,
            "replay receipt header mismatch",
        )
        acknowledgements = spark.read.json(str(acknowledgement_path)).select(
            "delivery_id",
            functions.col("partition").alias("ack_partition"),
            functions.col("offset").alias("ack_offset"),
        )
        terminal_offsets = spark.createDataFrame(
            [
                (int(partition), int(offset))
                for partition, offset in receipt["terminal_offsets"].items()
            ],
            ["terminal_partition", "terminal_offset"],
        )
    else:
        receipt_document = spark.read.option("multiline", "true").json(
            str(receipt_path)
        )
        receipt_header = receipt_document.select(
            "schema_version",
            "status",
            "replay_run_id",
            "replay_manifest_hash",
            "schedule_hash",
            "expected_delivery_count",
            "acknowledged_delivery_count",
        ).head()
        _require(receipt_header is not None, "replay receipt is empty")
        _require(
            receipt_header.schema_version == "replay-receipt-v1"
            and receipt_header.status == "complete"
            and receipt_header.replay_run_id == manifest.replay_run_id
            and receipt_header.replay_manifest_hash == manifest_hash
            and receipt_header.schedule_hash == manifest.schedule_hash
            and receipt_header.expected_delivery_count
            == manifest.expected_delivery_count
            and receipt_header.acknowledged_delivery_count
            == manifest.expected_delivery_count,
            "replay receipt header mismatch",
        )
        acknowledgements = receipt_document.select(
            functions.explode("acknowledgements").alias("ack")
        ).select(
            functions.col("ack.delivery_id").alias("delivery_id"),
            functions.col("ack.partition").alias("ack_partition"),
            functions.col("ack.offset").alias("ack_offset"),
        )
        terminal_offsets = receipt_document.select(
            functions.explode(
                functions.map_entries(
                    functions.from_json(
                        functions.to_json("terminal_offsets"),
                        MapType(StringType(), LongType()),
                    )
                )
            ).alias("terminal")
        ).select(
            functions.col("terminal.key").cast("int").alias("terminal_partition"),
            functions.col("terminal.value").alias("terminal_offset"),
        )
    _require(
        acknowledgements.count() == manifest.expected_delivery_count,
        "replay acknowledgement count mismatch",
    )
    _require_unique(
        acknowledgements,
        "delivery_id",
        "replay acknowledgement IDs are not unique",
    )
    _require_empty(
        schedule.select("delivery_id").join(
            acknowledgements.select("delivery_id"), "delivery_id", "left_anti"
        ),
        "replay receipt is missing scheduled deliveries",
    )
    _require_empty(
        acknowledgements.select("delivery_id").join(
            schedule.select("delivery_id"), "delivery_id", "left_anti"
        ),
        "replay receipt contains unexpected deliveries",
    )
    _require(
        terminal_offsets.count() == manifest.expected_terminal_count,
        "replay receipt terminal coverage mismatch",
    )

    bound_layers = str(Path(layers_root) / manifest_hash)
    bronze = spark.read.parquet(str(Path(bound_layers) / "bronze"))
    terminals = spark.read.parquet(str(Path(bound_layers) / "terminals"))
    _require(
        bronze.count() == manifest.expected_delivery_count,
        "Bronze delivery count does not equal acknowledged deliveries",
    )
    bronze_data = bronze.select(
        "value",
        functions.col("partition").alias("bronze_partition"),
        functions.col("offset").alias("bronze_offset"),
        functions.element_at("header_map", functions.lit("delivery_id")).alias(
            "delivery_id"
        ),
        functions.element_at("header_map", functions.lit("replay_manifest_hash")).alias(
            "bronze_manifest_hash"
        ),
        functions.element_at("header_map", functions.lit("source_event_id")).alias(
            "header_source_event_id"
        ),
        functions.element_at("header_map", functions.lit("injection_type")).alias(
            "header_injection_type"
        ),
        functions.element_at("header_map", functions.lit("payload_sha256")).alias(
            "header_payload_sha256"
        ),
    )
    _require_unique(
        bronze_data,
        "delivery_id",
        "Bronze delivery IDs are not unique",
    )
    _require_empty(
        bronze_data.where(
            ~functions.col("bronze_manifest_hash").eqNullSafe(
                functions.lit(manifest_hash)
            )
        ),
        "Bronze records are bound to another replay manifest",
    )
    joined = bronze_data.join(schedule, "delivery_id", "inner").join(
        acknowledgements, "delivery_id", "inner"
    )
    _require(
        joined.count() == manifest.expected_delivery_count,
        "Bronze, schedule, and receipt delivery coverage mismatch",
    )
    _require_empty(
        joined.where(
            (~functions.col("bronze_partition").eqNullSafe(functions.col("partition")))
            | (
                ~functions.col("bronze_partition").eqNullSafe(
                    functions.col("ack_partition")
                )
            )
            | (~functions.col("bronze_offset").eqNullSafe(functions.col("ack_offset")))
            | (
                ~functions.col("header_source_event_id").eqNullSafe(
                    functions.col("source_event_id")
                )
            )
            | (
                ~functions.col("header_injection_type").eqNullSafe(
                    functions.col("injection_type")
                )
            )
            | (
                ~functions.col("header_payload_sha256").eqNullSafe(
                    functions.col("payload_sha256")
                )
            )
            | (
                ~functions.sha2("value", 256).eqNullSafe(
                    functions.col("payload_sha256")
                )
            )
        ),
        "Bronze schedule, payload, or acknowledgement mismatch",
    )

    _require(
        terminals.count() == manifest.expected_terminal_count,
        "terminal record count mismatch",
    )
    terminal_data = terminals.select(
        functions.col("partition").alias("observed_partition"),
        functions.col("offset").alias("observed_offset"),
        "value",
        functions.element_at("header_map", functions.lit("replay_manifest_hash")).alias(
            "terminal_manifest_hash"
        ),
    )
    _require_unique(
        terminal_data,
        "observed_partition",
        "terminal partitions are not unique",
    )
    observed_partition_counts = (
        schedule.groupBy("partition")
        .count()
        .select(
            functions.col("partition").alias("scheduled_partition"),
            functions.col("count").alias("scheduled_partition_delivery_count"),
        )
    )
    scheduled_partition_counts = (
        spark.range(manifest.partition_count)
        .select(functions.col("id").cast("int").alias("scheduled_partition"))
        .join(observed_partition_counts, "scheduled_partition", "left")
        .fillna({"scheduled_partition_delivery_count": 0})
    )
    terminal_validation = (
        terminal_data.join(
            terminal_offsets,
            functions.col("observed_partition") == functions.col("terminal_partition"),
            "left",
        )
        .join(
            scheduled_partition_counts,
            functions.col("observed_partition") == functions.col("scheduled_partition"),
            "left",
        )
        .select(
            "*",
            functions.get_json_object("value", "$.record_type").alias(
                "payload_record_type"
            ),
            functions.get_json_object("value", "$.replay_manifest_hash").alias(
                "payload_manifest_hash"
            ),
            functions.get_json_object("value", "$.schedule_hash").alias(
                "payload_schedule_hash"
            ),
            functions.get_json_object("value", "$.partition")
            .cast("int")
            .alias("payload_partition"),
            functions.get_json_object("value", "$.expected_partition_delivery_count")
            .cast("long")
            .alias("payload_partition_delivery_count"),
        )
    )
    _require_empty(
        terminal_validation.where(
            (
                ~functions.col("terminal_manifest_hash").eqNullSafe(
                    functions.lit(manifest_hash)
                )
            )
            | (
                ~functions.col("payload_record_type").eqNullSafe(
                    functions.lit("terminal")
                )
            )
            | (
                ~functions.col("payload_manifest_hash").eqNullSafe(
                    functions.lit(manifest_hash)
                )
            )
            | (
                ~functions.col("payload_schedule_hash").eqNullSafe(
                    functions.lit(manifest.schedule_hash)
                )
            )
            | (
                ~functions.col("payload_partition").eqNullSafe(
                    functions.col("observed_partition")
                )
            )
            | (
                ~functions.col("observed_offset").eqNullSafe(
                    functions.col("terminal_offset")
                )
            )
            | (
                ~functions.col("payload_partition_delivery_count").eqNullSafe(
                    functions.col("scheduled_partition_delivery_count")
                )
            )
        ),
        "terminal payload, offset, or partition accounting mismatch",
    )

    candidates = joined.withColumn("event", functions.from_json("value", schema))
    _require_empty(
        candidates.where(
            (functions.col("injection_type") == "malformed")
            & functions.col("event").isNotNull()
        ),
        "scheduled malformed delivery contains a canonical event",
    )
    valid_candidates = candidates.where(functions.col("injection_type") != "malformed")
    _require_empty(
        valid_candidates.where(functions.col("event.source_event_id").isNull()),
        "scheduled valid delivery is not a canonical event",
    )
    valid_candidates = valid_candidates.join(
        canonical.select(
            functions.col("source_event_id").alias("canonical_source_event_id"),
            "canonical_payload_sha256",
        ),
        functions.col("event.source_event_id")
        == functions.col("canonical_source_event_id"),
        "left",
    )
    _require_empty(
        valid_candidates.where(
            functions.col("canonical_source_event_id").isNull()
            | (
                ~functions.col("event.source_event_id").eqNullSafe(
                    functions.col("source_event_id")
                )
            )
            | (
                ~functions.sha2("value", 256).eqNullSafe(
                    functions.col("canonical_payload_sha256")
                )
            )
        ),
        "valid Bronze payload differs from the canonical snapshot",
    )
    selection = Window.partitionBy("event.source_event_id").orderBy(
        "bronze_partition", "bronze_offset"
    )
    selected = (
        valid_candidates.withColumn(
            "selection_rank", functions.row_number().over(selection)
        )
        .where(functions.col("selection_rank") == 1)
        .select("value", "bronze_partition", "bronze_offset", "delivery_id", "event.*")
    )
    _require(
        selected.count() == canonical_count,
        "Gold does not cover every canonical source event exactly once",
    )
    _require_unique(selected, "source_event_id", "Gold source IDs are not unique")

    dimensions = selected.groupBy("meter_id", "unit", "decimal_scale").agg(
        functions.countDistinct("measurement_kind").alias("measurement_kind_count")
    )
    _require_empty(
        dimensions.where(functions.col("measurement_kind_count") != 1),
        "Gold aggregate group mixes measurement semantics",
    )
    aggregates = (
        selected.groupBy("meter_id", "measurement_kind", "unit", "decimal_scale")
        .agg(
            functions.count(functions.lit(1)).alias("count"),
            functions.sum(functions.col("scaled_value").cast("decimal(38,0)")).alias(
                "sum_scaled"
            ),
            functions.min("event_time_utc").alias("min_event_time"),
            functions.max("event_time_utc").alias("max_event_time"),
            functions.sum(functions.size("quality_flags"))
            .cast("long")
            .alias("quality_flag_count"),
        )
        .withColumn(
            "group_key",
            functions.concat_ws("|", "meter_id", "unit", "decimal_scale"),
        )
        .select(
            "group_key",
            "meter_id",
            "measurement_kind",
            "unit",
            "decimal_scale",
            "count",
            "sum_scaled",
            "min_event_time",
            "max_event_time",
            "quality_flag_count",
        )
    )
    aggregate_count = aggregates.count()

    stage = Path(tempfile.mkdtemp(prefix=".gold.", dir=run_dir))
    try:
        event_fields = schema.fieldNames()
        selected.select(*event_fields).write.mode("errorifexists").parquet(
            str(stage / "source-events.parquet")
        )
        aggregates.write.mode("errorifexists").parquet(
            str(stage / "aggregates.parquet")
        )
        selected.orderBy("source_event_id").select("value").coalesce(1).write.mode(
            "errorifexists"
        ).text(str(stage / ".source-events-ndjson"))
        aggregate_json = aggregates.orderBy("group_key").select(
            functions.to_json(
                functions.struct(
                    *[functions.col(field) for field in aggregates.columns]
                )
            ).alias("value")
        )
        aggregate_json.coalesce(1).write.mode("errorifexists").text(
            str(stage / ".aggregates-ndjson")
        )
        _move_single_part(
            stage / ".source-events-ndjson",
            stage / "gold-source-events.ndjson",
        )
        _move_single_part(
            stage / ".aggregates-ndjson",
            stage / "gold-aggregates.ndjson",
        )
        gold_manifest: dict[str, Any] = {
            "schema_version": "distributed-gold-manifest-v1",
            "status": "gold_finalized",
            "replay_run_id": manifest.replay_run_id,
            "replay_manifest_hash": manifest_hash,
            "schedule_hash": manifest.schedule_hash,
            "bronze_delivery_count": manifest.expected_delivery_count,
            "terminal_record_count": manifest.expected_terminal_count,
            "valid_delivery_count": valid_candidates.count(),
            "invalid_delivery_count": candidates.where(
                functions.col("injection_type") == "malformed"
            ).count(),
            "duplicate_valid_delivery_count": valid_candidates.count()
            - canonical_count,
            "source_coverage_count": canonical_count,
            "aggregate_count": aggregate_count,
            "gold_finalization_seconds": time.perf_counter() - started,
            "artifacts": {
                "gold_source_events_ndjson": {
                    "file": "gold-source-events.ndjson",
                    "record_count": canonical_count,
                    "byte_size": (stage / "gold-source-events.ndjson").stat().st_size,
                    "sha256": _file_sha256(stage / "gold-source-events.ndjson"),
                },
                "gold_aggregates_ndjson": {
                    "file": "gold-aggregates.ndjson",
                    "record_count": aggregate_count,
                    "byte_size": (stage / "gold-aggregates.ndjson").stat().st_size,
                    "sha256": _file_sha256(stage / "gold-aggregates.ndjson"),
                },
                "source_events_parquet": _directory_manifest(
                    stage / "source-events.parquet"
                ),
                "aggregates_parquet": _directory_manifest(stage / "aggregates.parquet"),
            },
        }
        write_json(gold_manifest, stage / "gold-manifest.json")
        if gold_root.exists():
            raise FileExistsError(f"immutable Gold output already exists: {gold_root}")
        stage.replace(gold_root)
        return gold_manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--layers-root", required=True)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("iot-energy-finalize-gold").getOrCreate()
    try:
        manifest = finalize_distributed_gold(
            spark,
            run_dir=args.run_dir,
            layers_root=args.layers_root,
        )
        print(json.dumps(manifest, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

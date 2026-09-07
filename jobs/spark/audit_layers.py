"""Account for every Bronze delivery after bounded streaming completes."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from iot_energy_pipeline.replay import write_json
from jobs.spark.common import canonical_spark_schema


def _require_empty(frame: Any, message: str) -> None:
    if frame.limit(1).count():
        raise ValueError(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _move_single_part(source: Path, destination: Path) -> None:
    parts = sorted(source.glob("part-*"))
    if len(parts) != 1:
        raise ValueError(f"expected one Spark output part in {source}")
    parts[0].replace(destination)
    shutil.rmtree(source)


def audit_stream_layers(
    spark: Any,
    *,
    layers_root: str,
    output_root: Path,
    replay_manifest_hash: str,
) -> dict[str, Any]:
    """Create an exact disposition for every data record archived in Bronze."""

    started = time.perf_counter()
    from pyspark.sql import functions as functions
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    if len(replay_manifest_hash) != 64 or any(
        character not in "0123456789abcdef" for character in replay_manifest_hash
    ):
        raise ValueError("replay_manifest_hash must be a lowercase SHA-256 digest")
    bound_layers = str(Path(layers_root) / replay_manifest_hash)
    audit_root = output_root
    if audit_root.exists():
        raise FileExistsError(f"immutable layer audit already exists: {audit_root}")

    delivery_key_fields = [
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
    ]
    bronze_schema = StructType(
        [
            *delivery_key_fields,
            StructField(
                "header_map",
                MapType(StringType(), StringType(), True),
                True,
            ),
        ]
    )
    parsed_schema = StructType(
        [
            *delivery_key_fields,
            StructField("source_event_id", StringType(), True),
        ]
    )
    quarantine_schema = StructType(
        [
            *delivery_key_fields,
            StructField("quarantine_category", StringType(), True),
        ]
    )
    silver_schema = StructType([*delivery_key_fields, *canonical_spark_schema().fields])
    terminal_schema = StructType([StructField("partition", IntegerType(), True)])

    bronze = spark.read.schema(bronze_schema).parquet(
        str(Path(bound_layers) / "bronze")
    )
    parsed = spark.read.schema(parsed_schema).parquet(
        str(Path(bound_layers) / "parsed")
    )
    quarantine = spark.read.schema(quarantine_schema).parquet(
        str(Path(bound_layers) / "quarantine")
    )
    silver = spark.read.schema(silver_schema).parquet(
        str(Path(bound_layers) / "silver")
    )
    terminals = spark.read.schema(terminal_schema).parquet(
        str(Path(bound_layers) / "terminals")
    )

    bronze_keys = bronze.select(
        functions.col("partition").alias("delivery_partition"),
        functions.col("offset").alias("delivery_offset"),
        functions.element_at("header_map", functions.lit("delivery_id")).alias(
            "delivery_id"
        ),
        functions.element_at("header_map", functions.lit("source_event_id")).alias(
            "header_source_event_id"
        ),
        functions.element_at("header_map", functions.lit("injection_type")).alias(
            "injection_type"
        ),
    )
    _require_empty(
        bronze_keys.groupBy("delivery_partition", "delivery_offset")
        .count()
        .where(functions.col("count") != 1),
        "Bronze partition/offset keys are not unique",
    )
    _require_empty(
        bronze_keys.groupBy("delivery_id").count().where(functions.col("count") != 1),
        "Bronze delivery IDs are not unique",
    )
    parsed_keys = parsed.select(
        functions.col("partition").alias("parsed_partition"),
        functions.col("offset").alias("parsed_offset"),
        functions.col("source_event_id").alias("parsed_source_event_id"),
    )
    quarantine_keys = quarantine.select(
        functions.col("partition").alias("quarantine_partition"),
        functions.col("offset").alias("quarantine_offset"),
        "quarantine_category",
    )
    silver_keys = silver.select(
        functions.col("partition").alias("silver_partition"),
        functions.col("offset").alias("silver_offset"),
        functions.col("source_event_id").alias("silver_source_event_id"),
    )
    _require_empty(
        silver_keys.groupBy("silver_source_event_id")
        .count()
        .where(functions.col("count") != 1),
        "Silver contains duplicate source_event_id values",
    )
    silver_sources = silver_keys.select(
        functions.col("silver_source_event_id").alias("retained_source_event_id")
    ).distinct()
    classified = (
        bronze_keys.join(
            parsed_keys,
            (functions.col("delivery_partition") == functions.col("parsed_partition"))
            & (functions.col("delivery_offset") == functions.col("parsed_offset")),
            "left",
        )
        .join(
            quarantine_keys,
            (
                functions.col("delivery_partition")
                == functions.col("quarantine_partition")
            )
            & (functions.col("delivery_offset") == functions.col("quarantine_offset")),
            "left",
        )
        .join(
            silver_keys,
            (functions.col("delivery_partition") == functions.col("silver_partition"))
            & (functions.col("delivery_offset") == functions.col("silver_offset")),
            "left",
        )
        .join(
            silver_sources,
            functions.col("parsed_source_event_id")
            == functions.col("retained_source_event_id"),
            "left",
        )
        .withColumn(
            "disposition",
            functions.when(
                functions.col("quarantine_category").isNotNull(),
                functions.concat(
                    functions.lit("quarantine:"),
                    functions.col("quarantine_category"),
                ),
            )
            .when(
                functions.col("silver_partition").isNotNull(),
                functions.lit("silver_selected"),
            )
            .when(
                functions.col("parsed_source_event_id").isNotNull()
                & functions.col("retained_source_event_id").isNotNull(),
                functions.lit("duplicate_valid"),
            )
            .when(
                functions.col("parsed_source_event_id").isNotNull(),
                functions.lit("late_valid"),
            ),
        )
    )
    _require_empty(
        classified.where(functions.col("disposition").isNull()),
        "one or more Bronze deliveries have no terminal disposition",
    )
    _require_empty(
        classified.where(
            functions.col("parsed_source_event_id").isNotNull()
            & functions.col("quarantine_category").isNotNull()
        ),
        "a delivery appears in both parsed and quarantine layers",
    )
    dispositions = classified.select(
        "delivery_partition",
        "delivery_offset",
        "delivery_id",
        "header_source_event_id",
        "injection_type",
        "disposition",
        "quarantine_category",
    )
    counts_by_disposition = {
        str(row["disposition"]): int(row["count"])
        for row in dispositions.groupBy("disposition").count().collect()
    }
    quarantine_count = sum(
        count
        for disposition, count in counts_by_disposition.items()
        if disposition.startswith("quarantine:")
    )
    counts = {
        "bronze_deliveries": bronze_keys.count(),
        "parsed_valid_deliveries": parsed_keys.count(),
        "quarantine_deliveries": quarantine_count,
        "silver_deliveries": counts_by_disposition.get("silver_selected", 0),
        "duplicate_valid_deliveries": counts_by_disposition.get("duplicate_valid", 0),
        "late_valid_deliveries": counts_by_disposition.get("late_valid", 0),
        "terminal_records": terminals.count(),
    }
    if counts["bronze_deliveries"] != (
        counts["quarantine_deliveries"]
        + counts["silver_deliveries"]
        + counts["duplicate_valid_deliveries"]
        + counts["late_valid_deliveries"]
    ):
        raise ValueError("Bronze layer conservation failed")
    if counts["bronze_deliveries"] != (
        counts["parsed_valid_deliveries"] + counts["quarantine_deliveries"]
    ):
        raise ValueError("parsed/quarantine layer conservation failed")

    audit_root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".audit.", dir=audit_root.parent))
    try:
        dispositions.write.mode("errorifexists").parquet(
            str(stage / "layer-dispositions.parquet")
        )
        dispositions.orderBy("delivery_partition", "delivery_offset").select(
            functions.to_json(
                functions.struct(
                    *[functions.col(field) for field in dispositions.columns]
                )
            ).alias("value")
        ).coalesce(1).write.mode("errorifexists").text(
            str(stage / ".layer-dispositions-ndjson")
        )
        _move_single_part(
            stage / ".layer-dispositions-ndjson",
            stage / "layer-dispositions.ndjson",
        )
        canonical_fields = canonical_spark_schema().fieldNames()
        silver.orderBy("source_event_id").select(
            functions.to_json(
                functions.struct(*[functions.col(field) for field in canonical_fields])
            ).alias("value")
        ).coalesce(1).write.mode("errorifexists").text(
            str(stage / ".silver-candidate-events-ndjson")
        )
        _move_single_part(
            stage / ".silver-candidate-events-ndjson",
            stage / "silver-candidate-events.ndjson",
        )
        manifest: dict[str, Any] = {
            "schema_version": "layer-audit-manifest-v1",
            "status": "complete",
            "replay_manifest_hash": replay_manifest_hash,
            "counts": counts,
            "disposition_counts": counts_by_disposition,
            "layer_audit_seconds": time.perf_counter() - started,
            "artifacts": {
                "layer_dispositions_ndjson": {
                    "file": "layer-dispositions.ndjson",
                    "record_count": counts["bronze_deliveries"],
                    "byte_size": (stage / "layer-dispositions.ndjson").stat().st_size,
                    "sha256": _sha256(stage / "layer-dispositions.ndjson"),
                },
                "silver_candidate_events_ndjson": {
                    "file": "silver-candidate-events.ndjson",
                    "record_count": counts["silver_deliveries"],
                    "byte_size": (stage / "silver-candidate-events.ndjson")
                    .stat()
                    .st_size,
                    "sha256": _sha256(stage / "silver-candidate-events.ndjson"),
                },
            },
        }
        write_json(manifest, stage / "layer-audit-manifest.json")
        if audit_root.exists():
            raise FileExistsError(f"immutable layer audit already exists: {audit_root}")
        stage.replace(audit_root)
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layers-root", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--replay-manifest-hash", required=True)
    args = parser.parse_args()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("iot-energy-layer-audit").getOrCreate()
    try:
        manifest = audit_stream_layers(
            spark,
            layers_root=args.layers_root,
            output_root=args.output_root,
            replay_manifest_hash=args.replay_manifest_hash,
        )
        print(json.dumps(manifest, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

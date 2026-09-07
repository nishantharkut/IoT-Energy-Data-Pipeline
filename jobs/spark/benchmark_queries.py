"""Benchmark the five declared read-only SQL workloads on finalized artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from iot_energy_pipeline.reports import summarize

QUERY_NAMES = ("meter", "building", "time_window", "quality", "exposure")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _write_atomic_new(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable query benchmark already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            raise FileExistsError(f"immutable query benchmark already exists: {path}")
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _exclusive_end(timestamp: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return (parsed + timedelta(microseconds=1)).isoformat().replace("+00:00", "Z")


def benchmark_queries(
    *,
    gold_dir: Path,
    decision_report_path: Path,
    queries_dir: Path,
    output: Path,
    repetitions: int = 3,
) -> dict[str, Any]:
    """Materialize and time every declared SQL query with stable parameters."""

    if (
        isinstance(repetitions, bool)
        or not isinstance(repetitions, int)
        or repetitions < 1
    ):
        raise ValueError("repetitions must be a positive integer")
    if output.exists():
        raise FileExistsError(f"immutable query benchmark already exists: {output}")
    gold_manifest_path = gold_dir / "gold-manifest.json"
    gold_manifest = _json_object(gold_manifest_path, "Gold manifest")
    decision = _json_object(decision_report_path, "decision-impact report")
    if (
        gold_manifest.get("schema_version") != "distributed-gold-manifest-v1"
        or gold_manifest.get("status") != "gold_finalized"
    ):
        raise ValueError("query benchmarks require finalized Gold")
    if (
        decision.get("schema_version") != "decision-impact-v1"
        or decision.get("status") != "verified"
        or decision.get("run_id") != gold_manifest.get("replay_run_id")
    ):
        raise ValueError("query benchmarks require a matching verified impact report")
    query_paths = {name: queries_dir / f"{name}.sql" for name in QUERY_NAMES}
    if any(not path.is_file() or path.is_symlink() for path in query_paths.values()):
        raise ValueError("one or more declared query files are missing")

    try:
        from pyspark.sql import SparkSession, functions
    except ImportError as exc:  # pragma: no cover - optional dependency gate
        raise RuntimeError("spark extra is required for query benchmarks") from exc
    spark = SparkSession.builder.appName("iot-energy-query-benchmark").getOrCreate()
    try:
        events = spark.read.parquet(str(gold_dir / "source-events.parquet")).cache()
        if events.count() < 1:
            raise ValueError("Gold contains no source events to query")
        first = events.orderBy("meter_id", "event_time_utc", "source_event_id").first()
        building = (
            events.select(functions.explode("brick_building_ids").alias("building_id"))
            .orderBy("building_id")
            .first()
        )
        bounds = events.agg(
            functions.min("event_time_utc").alias("minimum"),
            functions.max("event_time_utc").alias("maximum"),
        ).first()
        parameters = {
            "meter_id": str(first["meter_id"]),
            "building_id": "" if building is None else str(building["building_id"]),
            "window_start_utc": str(bounds["minimum"]),
            "window_end_utc": _exclusive_end(str(bounds["maximum"])),
        }
        spark.createDataFrame([parameters]).createOrReplaceTempView("query_parameters")
        scenarios = decision.get("tariff_scenarios")
        if not isinstance(scenarios, list) or not scenarios:
            raise ValueError("decision-impact tariff scenarios are missing")
        spark.createDataFrame(scenarios).createOrReplaceTempView(
            "decision_impact_scenarios"
        )
        events.createOrReplaceTempView("gold_source_events")

        results: list[dict[str, Any]] = []
        for name in QUERY_NAMES:
            sql = query_paths[name].read_text(encoding="utf-8")
            measurements: list[float] = []
            row_counts: list[int] = []
            for _ in range(repetitions):
                started = time.perf_counter()
                row_counts.append(spark.sql(sql).count())
                measurements.append(time.perf_counter() - started)
            if len(set(row_counts)) != 1:
                raise ValueError(f"query {name} returned unstable row counts")
            results.append(
                {
                    "query": name,
                    "repetitions": repetitions,
                    "row_count": row_counts[0],
                    "seconds": summarize(measurements),
                    "sql_sha256": _sha256(query_paths[name]),
                }
            )
        report: dict[str, Any] = {
            "schema_version": "query-benchmark-v1",
            "status": "complete",
            "run_id": gold_manifest["replay_run_id"],
            "gold_manifest_sha256": _sha256(gold_manifest_path),
            "decision_impact_sha256": _sha256(decision_report_path),
            "parameters": parameters,
            "results": results,
        }
        _write_atomic_new(output, report)
        return report
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-dir", type=Path, required=True)
    parser.add_argument("--decision-report", type=Path, required=True)
    parser.add_argument("--queries-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    report = benchmark_queries(
        gold_dir=args.gold_dir,
        decision_report_path=args.decision_report,
        queries_dir=args.queries_dir,
        output=args.output,
        repetitions=args.repetitions,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

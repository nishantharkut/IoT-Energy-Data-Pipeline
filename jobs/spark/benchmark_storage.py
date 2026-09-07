"""Measure NDJSON and Zstandard Parquet storage for a selected run snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from jobs.spark.common import canonical_spark_schema


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_measurement(path: Path) -> dict[str, int]:
    files = []
    for item in path.rglob("*"):
        if item.is_symlink():
            raise ValueError("storage benchmark output contains a symbolic link")
        if item.is_file():
            files.append(item)
    return {
        "byte_size": sum(item.stat().st_size for item in files),
        "file_count": len(files),
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def benchmark_storage(
    *,
    run_id: str,
    canonical_ndjson: Path,
    expected_sha256: str,
    output_root: Path,
) -> dict[str, Any]:
    """Write both Parquet layouts and report physical bytes; launch no other jobs."""

    if output_root.exists():
        raise FileExistsError(f"immutable storage benchmark exists: {output_root}")
    if (
        len(expected_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha256)
        or not canonical_ndjson.is_file()
        or canonical_ndjson.is_symlink()
        or _sha256(canonical_ndjson) != expected_sha256
    ):
        raise ValueError("canonical NDJSON integrity check failed")
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - optional dependency gate
        raise RuntimeError("spark extra is required for storage benchmarks") from exc

    output_root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(
        tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent)
    )
    spark = SparkSession.builder.appName("iot-energy-storage-benchmark").getOrCreate()
    try:
        source = (
            spark.read.schema(canonical_spark_schema())
            .option("mode", "FAILFAST")
            .json(str(canonical_ndjson))
            .cache()
        )
        record_count = source.count()
        if record_count < 1:
            raise ValueError("canonical NDJSON contains no records")
        unpartitioned = stage / "parquet-zstd-unpartitioned"
        started = time.perf_counter()
        source.write.mode("errorifexists").option("compression", "zstd").parquet(
            str(unpartitioned)
        )
        unpartitioned_seconds = time.perf_counter() - started

        partitioned = stage / "parquet-zstd-partitioned"
        started = time.perf_counter()
        source.write.mode("errorifexists").option("compression", "zstd").partitionBy(
            "source_variant", "meter_id"
        ).parquet(str(partitioned))
        partitioned_seconds = time.perf_counter() - started
        ndjson_bytes = canonical_ndjson.stat().st_size
        unpartitioned_measurement = _directory_measurement(unpartitioned)
        partitioned_measurement = _directory_measurement(partitioned)
        manifest: dict[str, Any] = {
            "schema_version": "storage-benchmark-v1",
            "status": "complete",
            "run_id": run_id,
            "record_count": record_count,
            "formats": {
                "ndjson": {
                    "byte_size": ndjson_bytes,
                    "file_count": 1,
                    "sha256": expected_sha256,
                    "write_seconds": None,
                    "relative_to_ndjson": 1.0,
                },
                "parquet_zstd_unpartitioned": {
                    **unpartitioned_measurement,
                    "write_seconds": unpartitioned_seconds,
                    "relative_to_ndjson": (
                        unpartitioned_measurement["byte_size"] / ndjson_bytes
                    ),
                },
                "parquet_zstd_partitioned": {
                    **partitioned_measurement,
                    "partition_columns": ["source_variant", "meter_id"],
                    "write_seconds": partitioned_seconds,
                    "relative_to_ndjson": (
                        partitioned_measurement["byte_size"] / ndjson_bytes
                    ),
                },
            },
        }
        _write_json(stage / "storage-benchmark.json", manifest)
        if output_root.exists():
            raise FileExistsError(f"immutable storage benchmark exists: {output_root}")
        stage.replace(output_root)
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    finally:
        spark.stop()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--canonical-ndjson", type=Path, required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    report = benchmark_storage(
        run_id=args.run_id,
        canonical_ndjson=args.canonical_ndjson,
        expected_sha256=args.expected_sha256,
        output_root=args.output_root,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

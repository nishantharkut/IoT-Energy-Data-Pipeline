"""Coherent synthetic fixture acceptance flow across every semantic layer."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dashboard.data import load_finalized_run
from jobs.hadoop.reconcile import reconcile, reconcile_source_records
from jobs.spark.common import archive_kafka_record
from jobs.spark.finalize import finalize_run
from jobs.spark.stream import process_deliveries

from .canonicalize import (
    CanonicalizationRules,
    MeasurementRule,
    build_snapshot,
)
from .manifests import register_dataset
from .profile import profile_dataset
from .replay import ProduceRequest, execute_replay, make_manifest
from .reports import build_decision_impact_report
from .schedule import build_schedule
from .source_layout import SourceLayoutRules, WorkbookLayoutRule


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any, *, pretty: bool = True) -> bytes:
    if pretty:
        text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    else:
        text = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    return (text + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_json_bytes(value))


def _write_ndjson(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        for record in records:
            stream.write(_json_bytes(record, pretty=False))


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _create_fixture_source(root: Path) -> None:
    from openpyxl import Workbook

    root.mkdir(parents=True)
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Readings"
    sheet.append(["timestamp", "meter_id", "value"])
    start = datetime(2024, 1, 1)
    for index in range(48):
        timestamp = start + timedelta(minutes=15 * index)
        values = {
            "meter-a": 100 + index,
            "meter-b": 200 + index - (1 if index >= 10 else 0),
            "meter-c": 300 + index,
        }
        for meter_id in ("meter-a", "meter-b", "meter-c"):
            value: str | None = f"{values[meter_id]}.00"
            if meter_id == "meter-b" and index == 7:
                value = None
            sheet.append([timestamp, meter_id, value])
        if index == 5:
            sheet.append([timestamp, "meter-a", f"{values['meter-a']}.00"])
    workbook.save(root / "readings.xlsx")
    (root / "metadata.ttl").write_text(
        """\
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix unit: <http://qudt.org/vocab/unit/> .
@prefix ex: <https://example.test/> .
ex:meter-a a brick:Electrical_Meter ;
  brick:hasUnit unit:KiloW-HR ;
  ref:hasExternalReference [ ref:hasTimeseriesId "meter-a" ] .
ex:meter-b a brick:Electrical_Meter ;
  brick:hasUnit unit:KiloW-HR ;
  ref:hasExternalReference [ ref:hasTimeseriesId "meter-b" ] .
ex:building-a a brick:Building ; brick:hasPart ex:zone-a .
ex:building-b a brick:Building ; brick:hasPart ex:zone-b .
ex:zone-a a brick:Zone ; brick:hasPart ex:meter-a .
ex:zone-b a brick:Zone ; brick:hasPart ex:meter-b .
ex:panel-a a brick:Equipment ; brick:isMeteredBy ex:meter-a .
ex:panel-b a brick:Equipment ; brick:isMeteredBy ex:meter-b .
""",
        encoding="utf-8",
        newline="\n",
    )


def _fixture_rules() -> CanonicalizationRules:
    evidence = "synthetic fixture contract; not a claim about the public source"
    rule = MeasurementRule(
        measurement_kind="cumulative_energy",
        unit="kWh",
        decimal_scale=2,
        evidence=evidence,
        expected_brick_unit_suffix="KiloW-HR",
    )
    return CanonicalizationRules(
        source_timezone="Asia/Hong_Kong",
        timezone_evidence=evidence,
        source_layout=SourceLayoutRules(
            rules=(
                WorkbookLayoutRule(
                    name="synthetic-long-format",
                    relative_path_pattern=r"readings\.xlsx",
                    sheet_name="Readings",
                    header_row=1,
                    expected_columns=("timestamp", "meter_id", "value"),
                    timestamp_column="timestamp",
                    value_column="value",
                    meter_id_column="meter_id",
                    evidence=evidence,
                ),
            ),
            evidence=evidence,
        ),
        measurements={
            "meter-a": rule,
            "meter-b": rule,
            "meter-c": MeasurementRule(
                measurement_kind="cumulative_energy",
                unit="kWh",
                decimal_scale=2,
                evidence=evidence,
            ),
        },
    )


@dataclass
class _LocalLogProducer:
    produced: list[tuple[ProduceRequest, int]]
    next_offsets: dict[int, int]

    def __init__(self) -> None:
        self.produced = []
        self.next_offsets = {}

    def produce(self, request: ProduceRequest) -> int:
        offset = self.next_offsets.get(request.partition, 0)
        self.next_offsets[request.partition] = offset + 1
        self.produced.append((request, offset))
        return offset

    def flush(self) -> None:
        return None


def _bronze_records(
    produced: list[tuple[ProduceRequest, int]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    data: list[dict[str, Any]] = []
    terminals: list[dict[str, Any]] = []
    base = datetime(2024, 1, 2)
    for position, (request, offset) in enumerate(produced):
        timestamp = (base + timedelta(seconds=position)).isoformat() + "Z"
        record = archive_kafka_record(
            topic=request.topic,
            key=request.key,
            value=request.value,
            headers=request.headers,
            partition=request.partition,
            offset=offset,
            kafka_timestamp=timestamp,
            ingestion_timestamp=timestamp,
        )
        record["delivery_id"] = request.delivery_id
        if request.record_type == "terminal":
            terminals.append(record)
        else:
            data.append(record)
    return data, terminals


def _run_local_hadoop(
    source: Path,
    destination: Path,
    *,
    mapper_module: str = "jobs.hadoop.mapper",
    reducer_module: str = "jobs.hadoop.reducer",
) -> tuple[list[dict[str, Any]], float]:
    repository = Path(__file__).resolve().parents[2]
    started = time.perf_counter()
    mapper = subprocess.run(
        [sys.executable, "-m", mapper_module],
        input=source.read_text(encoding="utf-8"),
        text=True,
        capture_output=True,
        cwd=repository,
        check=False,
    )
    if mapper.returncode:
        raise RuntimeError(
            f"Hadoop mapper fixture failed ({mapper_module}): {mapper.stderr.strip()}"
        )
    mapped = "\n".join(sorted(mapper.stdout.splitlines())) + "\n"
    reducer = subprocess.run(
        [sys.executable, "-m", reducer_module],
        input=mapped,
        text=True,
        capture_output=True,
        cwd=repository,
        check=False,
    )
    if reducer.returncode:
        raise RuntimeError(
            f"Hadoop reducer fixture failed ({reducer_module}): "
            f"{reducer.stderr.strip()}"
        )
    elapsed = time.perf_counter() - started
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(reducer.stdout, encoding="utf-8", newline="\n")
    return _read_ndjson(destination), elapsed


def _artifact_registry(run_dir: Path) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(
        (item for item in run_dir.rglob("*") if item.is_file()),
        key=lambda item: item.relative_to(run_dir).as_posix(),
    ):
        relative = path.relative_to(run_dir).as_posix()
        if relative in {"run-manifest.json", "run-manifest.sha256"}:
            continue
        name = relative.replace("/", "__").replace(".", "_")
        registry[name] = {
            "relative_path": relative,
            "byte_size": path.stat().st_size,
            "sha256": _sha256(path),
        }
    return registry


def run_local_fixture(
    output_dir: Path,
    *,
    run_id: str = "fixture-combined",
    fault: str = "combined",
    watermark_seconds: int = 60,
    interrupt_after: int | None = None,
) -> dict[str, Any]:
    """Run one coherent local semantic fixture and seal its finalized outputs."""

    if output_dir.exists():
        raise FileExistsError(f"immutable fixture run already exists: {output_dir}")
    if watermark_seconds < 0:
        raise ValueError("watermark_seconds must be non-negative")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        source_root = stage / "source"
        _create_fixture_source(source_root)
        dataset_manifest = register_dataset(
            source_root,
            stage / "dataset-manifest.json",
            dataset_version="synthetic-fixture-v1",
            source_variant="clean",
        )
        profile_dataset(dataset_manifest, stage / "dataset-profile.json")
        snapshot = build_snapshot(
            dataset_manifest, stage / "snapshot", _fixture_rules()
        )
        if snapshot.events is None:
            raise RuntimeError("fixture snapshot exceeded the materialization boundary")
        canonical = list(snapshot.events)
        schedule = build_schedule(
            canonical,
            run_id,
            seed=20260825,
            partition_count=3,
            fault=fault,
        )
        replay_manifest = make_manifest(
            schedule,
            snapshot.manifest["artifacts"]["canonical_ndjson"]["sha256"],
            requested_event_count=len(canonical),
        )
        producer = _LocalLogProducer()
        replay_root = stage / "replay"
        replay_topic = "iot-energy-fixture"
        replay_manifest_path = replay_root / "replay-manifest.json"
        replay_progress_path = replay_root / "replay-progress.json"
        replay_receipt_path = replay_root / "replay-receipt.json"
        if interrupt_after is not None:
            interrupted = execute_replay(
                schedule,
                replay_manifest,
                producer,
                topic=replay_topic,
                manifest_path=replay_manifest_path,
                progress_path=replay_progress_path,
                receipt_path=replay_receipt_path,
                interrupt_after=interrupt_after,
            )
            if interrupted is not None:
                raise RuntimeError("fixture interruption did not stop replay")
            receipt = execute_replay(
                schedule,
                replay_manifest,
                producer,
                topic=replay_topic,
                manifest_path=replay_manifest_path,
                progress_path=replay_progress_path,
                receipt_path=replay_receipt_path,
                resume=True,
            )
        else:
            receipt = execute_replay(
                schedule,
                replay_manifest,
                producer,
                topic=replay_topic,
                manifest_path=replay_manifest_path,
                progress_path=replay_progress_path,
                receipt_path=replay_receipt_path,
            )
        if receipt is None:
            raise RuntimeError("fixture replay did not produce a complete receipt")
        bronze, terminals = _bronze_records(producer.produced)
        _write_ndjson(stage / "layers/bronze.ndjson", bronze)
        _write_ndjson(stage / "layers/terminals.ndjson", terminals)
        known = {event["source_event_id"]: event for event in canonical}
        layers = process_deliveries(
            bronze,
            known,
            timedelta(seconds=watermark_seconds),
            replay_manifest.sha256(),
        )
        for name in (
            "parsed",
            "silver",
            "duplicate_valid",
            "late_valid",
            "quarantine",
        ):
            _write_ndjson(stage / f"layers/{name}.ndjson", layers[name])
        _write_json(stage / "layers/layer-counts.json", layers["counts"])

        gold = finalize_run(
            canonical,
            schedule,
            replay_manifest,
            receipt,
            bronze,
            terminals,
            stage / "gold",
        )
        aggregate_oracle_path = stage / "oracle/hadoop-aggregates.ndjson"
        aggregate_oracle, aggregate_seconds = _run_local_hadoop(
            stage / "snapshot/canonical.ndjson", aggregate_oracle_path
        )
        source_oracle_path = stage / "oracle/hadoop-source-records.ndjson"
        source_oracle, source_seconds = _run_local_hadoop(
            stage / "snapshot/canonical.ndjson",
            source_oracle_path,
            mapper_module="jobs.hadoop.record_mapper",
            reducer_module="jobs.hadoop.record_reducer",
        )
        aggregate_errors = reconcile(gold.aggregates, aggregate_oracle)
        source_errors = reconcile_source_records(gold.source_events, source_oracle)
        errors = [
            *(f"aggregate: {error}" for error in aggregate_errors),
            *(f"source: {error}" for error in source_errors),
        ]
        verification_seconds = aggregate_seconds + source_seconds
        reconciliation = {
            "schema_version": "reconciliation-v2",
            "status": "passed" if not errors else "failed",
            "spark_record_count": len(gold.aggregates),
            "hadoop_record_count": len(aggregate_oracle),
            "spark_source_record_count": len(gold.source_events),
            "hadoop_source_record_count": len(source_oracle),
            "exact_aggregate_fields": sorted(
                set().union(*(record.keys() for record in gold.aggregates))
            ),
            "exact_source_fields": sorted(
                set().union(*(record.keys() for record in gold.source_events))
            ),
            "errors": errors,
            "gold_sha256": _sha256(stage / "gold/gold-aggregates.ndjson"),
            "hadoop_sha256": _sha256(aggregate_oracle_path),
            "gold_source_sha256": _sha256(stage / "gold/gold-source-events.ndjson"),
            "hadoop_source_sha256": _sha256(source_oracle_path),
            "verification_seconds": verification_seconds,
            "oracle_input": "snapshot/canonical.ndjson",
        }
        _write_json(stage / "reports/reconciliation.json", reconciliation)
        if errors:
            raise ValueError("fixture Spark-Hadoop reconciliation failed")
        candidate_events = [item["event"] for item in layers["silver"]]
        candidate_bytes = len(_json_bytes(candidate_events, pretty=False))
        verification_bytes = (
            stage / "gold/gold-aggregates.ndjson"
        ).stat().st_size + aggregate_oracle_path.stat().st_size
        verification_bytes += source_oracle_path.stat().st_size
        impact = build_decision_impact_report(
            candidate_events,
            gold.source_events,
            reconciliation_passed=True,
            verification_seconds=verification_seconds,
            candidate_storage_bytes=candidate_bytes,
            verification_storage_bytes=verification_bytes,
        )
        _write_json(stage / "reports/decision-impact.json", impact)

        injection_counts = Counter(
            delivery.injection_type for delivery in schedule.deliveries
        )
        quarantine_counts = Counter(row["category"] for row in layers["quarantine"])
        finalized_manifest: dict[str, Any] = {
            "schema_version": "finalized-run-v1",
            "status": "finalized",
            "reconciled": True,
            "immutable": True,
            "replay_run_id": run_id,
            "fault": fault,
            "watermark_seconds": watermark_seconds,
            "interrupted_and_resumed": interrupt_after is not None,
            "artifacts": _artifact_registry(stage),
            "views": {
                "provenance_status": {
                    "title": "Provenance and status",
                    "rows": [
                        {
                            "run_id": run_id,
                            "dataset_version": dataset_manifest.dataset_version,
                            "source_variant": dataset_manifest.source_variant,
                            "snapshot_sha256": replay_manifest.snapshot_hash,
                            "replay_manifest_sha256": replay_manifest.sha256(),
                            "status": "finalized",
                        }
                    ],
                },
                "throughput_layers": {
                    "title": "Throughput and layer conservation",
                    "rows": [
                        {
                            **layers["counts"],
                            "acknowledged_deliveries": (
                                receipt.acknowledged_delivery_count
                            ),
                            "terminal_records": len(terminals),
                            "gold_source_events": len(gold.source_events),
                        }
                    ],
                },
                "fault_disposition": {
                    "title": "Injected fault disposition",
                    "rows": [
                        {
                            "injection_type": name,
                            "delivery_count": count,
                            "quarantine_count": quarantine_counts.get(
                                "invalid_json" if name == "malformed" else name,
                                0,
                            ),
                        }
                        for name, count in sorted(injection_counts.items())
                    ],
                },
                "energy_quality": {
                    "title": "Energy and data quality",
                    "rows": [
                        {
                            "gold_group_count": len(gold.aggregates),
                            "source_quality_rejections": (
                                snapshot.source_quality_count
                            ),
                            "affected_meter_periods": impact[
                                "affected_meter_period_count"
                            ],
                            "absolute_kwh_discrepancy": impact[
                                "absolute_kwh_discrepancy"
                            ],
                        }
                    ],
                },
                "storage_queries": {
                    "title": "Storage and verification measurements",
                    "rows": [
                        {
                            "canonical_ndjson_bytes": (
                                stage / "snapshot/canonical.ndjson"
                            )
                            .stat()
                            .st_size,
                            "canonical_parquet_bytes": (
                                stage / "snapshot/canonical.parquet"
                            )
                            .stat()
                            .st_size,
                            "verification_storage_bytes": verification_bytes,
                            "verification_seconds": verification_seconds,
                        }
                    ],
                },
                "reconciliation": {
                    "title": "Spark-Hadoop reconciliation",
                    "rows": [reconciliation],
                },
            },
        }
        manifest_payload = _json_bytes(finalized_manifest)
        (stage / "run-manifest.json").write_bytes(manifest_payload)
        (stage / "run-manifest.sha256").write_text(
            hashlib.sha256(manifest_payload).hexdigest() + "\n",
            encoding="ascii",
            newline="\n",
        )
        stage.replace(output_dir)
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return load_finalized_run(output_dir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", default="fixture-combined")
    parser.add_argument(
        "--fault",
        choices=("clean", "duplicate", "delayed", "malformed", "combined"),
        default="combined",
    )
    parser.add_argument("--watermark-seconds", type=int, default=60)
    parser.add_argument("--interrupt-after", type=int)
    args = parser.parse_args()
    run_local_fixture(
        args.output,
        run_id=args.run_id,
        fault=args.fault,
        watermark_seconds=args.watermark_seconds,
        interrupt_after=args.interrupt_after,
    )
    print(args.output / "run-manifest.json")


if __name__ == "__main__":
    main()

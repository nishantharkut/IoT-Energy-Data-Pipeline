from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from iot_energy_pipeline.replay import (
    ProduceRequest,
    execute_replay,
    make_manifest,
)
from iot_energy_pipeline.schedule import build_schedule
from jobs.spark.finalize import finalize_run


def _event(index: int) -> dict:
    return {
        "schema_version": "canonical-event-v1",
        "dataset_version": "fixture-v1",
        "source_variant": "clean",
        "source_event_id": hashlib.sha256(f"row-{index}".encode()).hexdigest(),
        "source_relative_path": "readings.xlsx",
        "workbook_sheet": "Readings",
        "source_row_number": index + 2,
        "meter_id": "meter-a",
        "brick_entity_id": "https://example.test/meter-a",
        "brick_building_ids": ["https://example.test/building-a"],
        "brick_zone_ids": ["https://example.test/zone-a"],
        "brick_metered_entities": [],
        "brick_unit_uri": "http://qudt.org/vocab/unit/KiloW-HR",
        "brick_usage_type": None,
        "original_timestamp_text": f"2024-01-01T0{index}:00:00",
        "source_timezone": "Asia/Hong_Kong",
        "event_time_utc": f"2024-01-01T0{index}:00:00Z",
        "original_numeric_text": f"{index}.00",
        "scaled_value": index * 100,
        "decimal_scale": 2,
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
        "quality_flags": [],
    }


@dataclass
class FakeProducer:
    requests: list[ProduceRequest]
    next_offsets: dict[int, int]

    def __init__(self) -> None:
        self.requests = []
        self.next_offsets = {}

    def produce(self, request: ProduceRequest) -> int:
        self.requests.append(request)
        offset = self.next_offsets.get(request.partition, 0)
        self.next_offsets[request.partition] = offset + 1
        return offset

    def flush(self) -> None:
        pass


def _run(tmp_path: Path, fault: str):
    canonical = [_event(index) for index in range(1, 4)]
    schedule = build_schedule(
        canonical, f"{fault}-fixture", seed=17, partition_count=2, fault=fault
    )
    manifest = make_manifest(schedule, "a" * 64, requested_event_count=3)
    producer = FakeProducer()
    receipt = execute_replay(
        schedule,
        manifest,
        producer,
        topic="fixture-events",
        manifest_path=tmp_path / fault / "replay-manifest.json",
        progress_path=tmp_path / fault / "progress.json",
        receipt_path=tmp_path / fault / "receipt.json",
    )
    assert receipt is not None
    bronze = []
    terminals = []
    offsets: dict[int, int] = {}
    for request in producer.requests:
        offset = offsets.get(request.partition, 0)
        offsets[request.partition] = offset + 1
        record = {
            "topic": request.topic,
            "key": request.key,
            "value": request.value,
            "headers": dict(request.headers),
            "partition": request.partition,
            "offset": offset,
            "delivery_id": request.delivery_id,
        }
        if request.record_type == "data":
            bronze.append(record)
        else:
            terminals.append(record)
    return canonical, schedule, manifest, receipt, bronze, terminals


def test_clean_and_combined_bronze_finalize_to_identical_semantic_gold(
    tmp_path: Path,
) -> None:
    clean = _run(tmp_path, "clean")
    combined = _run(tmp_path, "combined")

    clean_result = finalize_run(*clean[:4], clean[4], clean[5])
    combined_result = finalize_run(*combined[:4], combined[4], combined[5])

    assert clean_result.source_events == combined_result.source_events
    assert clean_result.aggregates == combined_result.aggregates
    assert clean_result.manifest["source_coverage_count"] == 3
    assert combined_result.manifest["bronze_delivery_count"] == len(combined[4])


def test_finalizer_selects_lowest_partition_offset_for_valid_duplicates(
    tmp_path: Path,
) -> None:
    canonical, schedule, manifest, receipt, bronze, terminals = _run(
        tmp_path, "duplicate"
    )

    result = finalize_run(canonical, schedule, manifest, receipt, bronze, terminals)

    selected = result.manifest["selected_deliveries"]
    for source_id, location in selected.items():
        candidates = [
            (record["partition"], record["offset"])
            for record in bronze
            if json.loads(record["value"])["source_event_id"] == source_id
        ]
        assert (location["partition"], location["offset"]) == min(candidates)


def test_finalizer_rejects_receipt_terminal_and_bronze_mismatches(
    tmp_path: Path,
) -> None:
    canonical, schedule, manifest, receipt, bronze, terminals = _run(tmp_path, "clean")

    with pytest.raises(ValueError, match="duplicate Bronze delivery_id"):
        finalize_run(
            canonical, schedule, manifest, receipt, bronze + [bronze[0]], terminals
        )
    with pytest.raises(ValueError, match="terminal partition coverage"):
        finalize_run(canonical, schedule, manifest, receipt, bronze, terminals[:-1])
    bad_receipt = type(receipt)(
        **{
            **receipt.__dict__,
            "acknowledged_delivery_ids": receipt.acknowledged_delivery_ids[:-1],
            "acknowledged_delivery_count": receipt.acknowledged_delivery_count - 1,
            "acknowledgements": receipt.acknowledgements[:-1],
        }
    )
    with pytest.raises(ValueError, match="receipt delivery coverage"):
        finalize_run(canonical, schedule, manifest, bad_receipt, bronze, terminals)


def test_finalizer_rejects_payload_mutation_unknown_sources_and_incomplete_coverage(
    tmp_path: Path,
) -> None:
    canonical, schedule, manifest, receipt, bronze, terminals = _run(tmp_path, "clean")
    mutated = [dict(record) for record in bronze]
    changed = json.loads(mutated[0]["value"])
    changed["scaled_value"] += 1
    mutated[0]["value"] = json.dumps(changed, sort_keys=True, separators=(",", ":"))

    with pytest.raises(ValueError, match="canonical payload digest"):
        finalize_run(canonical, schedule, manifest, receipt, mutated, terminals)

    unknown = [dict(record) for record in bronze]
    changed = json.loads(unknown[0]["value"])
    changed["source_event_id"] = "f" * 64
    unknown[0]["value"] = json.dumps(changed, sort_keys=True, separators=(",", ":"))
    with pytest.raises(ValueError, match="unknown source_event_id"):
        finalize_run(canonical, schedule, manifest, receipt, unknown, terminals)

    with pytest.raises(ValueError, match="incomplete source coverage"):
        finalize_run(
            canonical + [_event(4)],
            schedule,
            manifest,
            receipt,
            bronze,
            terminals,
        )


def test_finalized_outputs_are_immutable_and_hash_bound(tmp_path: Path) -> None:
    canonical, schedule, manifest, receipt, bronze, terminals = _run(tmp_path, "clean")
    output = tmp_path / "gold"

    result = finalize_run(
        canonical, schedule, manifest, receipt, bronze, terminals, output
    )

    saved = json.loads((output / "gold-manifest.json").read_text(encoding="utf-8"))
    assert saved == result.manifest
    assert saved["status"] == "gold_finalized"
    assert saved["artifacts"]["gold_aggregates"]["sha256"]
    with pytest.raises(FileExistsError, match="immutable Gold"):
        finalize_run(canonical, schedule, manifest, receipt, bronze, terminals, output)

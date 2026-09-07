from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from iot_energy_pipeline.replay import ProduceRequest, ReplayManifest
from iot_energy_pipeline.streaming_replay import (
    execute_streaming_replay,
    load_streaming_replay_receipt,
    load_streaming_schedule_metadata,
    write_streaming_schedule,
)


@dataclass
class FakeProducer:
    requests: list[ProduceRequest]
    next_offsets: dict[int, int]
    flush_count: int = 0

    def __init__(self) -> None:
        self.requests = []
        self.next_offsets = {}
        self.flush_count = 0

    def produce(self, request: ProduceRequest) -> int:
        self.requests.append(request)
        offset = self.next_offsets.get(request.partition, 0)
        self.next_offsets[request.partition] = offset + 1
        return offset

    def flush(self) -> None:
        self.flush_count += 1


def _canonical(path: Path) -> None:
    records = [
        {
            "schema_version": "canonical-event-v1",
            "source_event_id": f"{index:064x}",
            "event_time_utc": timestamp,
            "meter_id": "meter-a",
            "scaled_value": index,
        }
        for index, timestamp in (
            (1, "2024-01-01T02:00:00Z"),
            (2, "2024-01-01T00:00:00Z"),
            (3, "2024-01-01T01:00:00Z"),
        )
    ]
    path.write_text(
        "".join(
            json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n"
            for item in records
        ),
        encoding="utf-8",
    )


def _manifest(metadata: object, snapshot_hash: str) -> ReplayManifest:
    return ReplayManifest(
        schema_version="replay-manifest-v1",
        replay_run_id=metadata.run_id,
        snapshot_hash=snapshot_hash,
        schedule_hash=metadata.schedule_sha256,
        seed=metadata.seed,
        partition_count=metadata.partition_count,
        fault=metadata.fault,
        requested_event_count=3,
        repetition=1,
        expected_delivery_count=metadata.delivery_count,
        expected_terminal_count=metadata.partition_count,
        maximum_event_time_lateness_seconds=(
            metadata.maximum_event_time_lateness_seconds
        ),
    )


def test_streaming_schedule_is_deterministic_and_event_time_ordered(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical.ndjson"
    _canonical(canonical)
    first = tmp_path / "first.ndjson"
    second = tmp_path / "second.ndjson"

    metadata = write_streaming_schedule(
        canonical,
        first,
        run_id="scale-clean-r01",
        seed=7,
        partition_count=3,
        fault="clean",
    )
    repeated = write_streaming_schedule(
        canonical,
        second,
        run_id="scale-clean-r01",
        seed=7,
        partition_count=3,
        fault="clean",
    )

    deliveries = [json.loads(line) for line in first.read_text("utf-8").splitlines()]
    assert [item["event_time"] for item in deliveries] == sorted(
        item["event_time"] for item in deliveries
    )
    assert metadata == repeated
    assert first.read_bytes() == second.read_bytes()
    assert metadata.schedule_sha256 == hashlib.sha256(first.read_bytes()).hexdigest()
    for item in deliveries:
        expected_id = hashlib.sha256(
            (
                f"scale-clean-r01|7|{item['source_event_id']}|"
                f"{item['injection_type']}|{item['position']}"
            ).encode("utf-8")
        ).hexdigest()
        assert item["delivery_id"] == expected_id


def test_streaming_replay_resumes_from_hash_bound_acknowledgement_ledger(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical.ndjson"
    _canonical(canonical)
    schedule = tmp_path / "schedule.ndjson"
    metadata = write_streaming_schedule(
        canonical,
        schedule,
        run_id="scale-resume-r01",
        seed=11,
        partition_count=2,
        fault="duplicate",
    )
    metadata_path = tmp_path / "schedule-metadata.json"
    metadata_path.write_text(
        json.dumps(metadata.to_dict(), sort_keys=True), encoding="utf-8"
    )
    loaded = load_streaming_schedule_metadata(metadata_path, schedule_path=schedule)
    manifest = _manifest(loaded, hashlib.sha256(canonical.read_bytes()).hexdigest())
    producer = FakeProducer()
    kwargs = {
        "schedule_path": schedule,
        "metadata": loaded,
        "manifest": manifest,
        "producer": producer,
        "topic": "scale-events",
        "progress_path": tmp_path / "progress.json",
        "acknowledgement_path": tmp_path / "acknowledgements.ndjson",
        "receipt_path": tmp_path / "receipt.json",
    }

    assert execute_streaming_replay(interrupt_after=2, **kwargs) is None
    first_data_ids = [
        request.delivery_id
        for request in producer.requests
        if request.record_type == "data"
    ]
    receipt = execute_streaming_replay(resume=True, **kwargs)

    assert receipt is not None
    all_data_ids = [
        request.delivery_id
        for request in producer.requests
        if request.record_type == "data"
    ]
    assert all_data_ids[:2] == first_data_ids
    assert len(all_data_ids) == len(set(all_data_ids)) == metadata.delivery_count
    assert receipt["schema_version"] == "replay-receipt-v2"
    assert receipt["acknowledgement_ledger"]["record_count"] == metadata.delivery_count
    assert (
        load_streaming_replay_receipt(
            kwargs["receipt_path"], acknowledgement_path=kwargs["acknowledgement_path"]
        )
        == receipt
    )
    assert not kwargs["progress_path"].exists()


def test_streaming_replay_rejects_schedule_tampering_before_production(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical.ndjson"
    _canonical(canonical)
    schedule = tmp_path / "schedule.ndjson"
    metadata = write_streaming_schedule(
        canonical,
        schedule,
        run_id="scale-clean-r01",
        seed=7,
        partition_count=2,
        fault="clean",
    )
    schedule.write_text(schedule.read_text("utf-8") + "{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="schedule integrity"):
        execute_streaming_replay(
            schedule_path=schedule,
            metadata=metadata,
            manifest=_manifest(
                metadata, hashlib.sha256(canonical.read_bytes()).hexdigest()
            ),
            producer=FakeProducer(),
            topic="scale-events",
            progress_path=tmp_path / "progress.json",
            acknowledgement_path=tmp_path / "acknowledgements.ndjson",
            receipt_path=tmp_path / "receipt.json",
        )

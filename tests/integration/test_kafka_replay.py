from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from iot_energy_pipeline.replay import (
    ProduceRequest,
    execute_replay,
    load_progress,
    load_replay_manifest,
    load_replay_receipt,
    make_manifest,
)
from iot_energy_pipeline.schedule import build_schedule


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


def _events() -> list[dict]:
    return [
        {
            "schema_version": "canonical-event-v1",
            "source_event_id": f"{index:064x}",
            "event_time_utc": f"2024-01-01T0{index}:00:00Z",
            "meter_id": "meter-a",
            "scaled_value": index,
        }
        for index in range(1, 4)
    ]


def test_replay_writes_data_then_one_terminal_per_partition_and_receipt(
    tmp_path: Path,
) -> None:
    schedule = build_schedule(
        _events(), "combined-fixture", seed=7, partition_count=3, fault="combined"
    )
    manifest = make_manifest(schedule, "a" * 64, requested_event_count=3)
    producer = FakeProducer()
    manifest_path = tmp_path / "replay-manifest.json"
    receipt_path = tmp_path / "receipt.json"
    progress_path = tmp_path / "progress.json"

    receipt = execute_replay(
        schedule,
        manifest,
        producer,
        topic="fixture-events",
        manifest_path=manifest_path,
        progress_path=progress_path,
        receipt_path=receipt_path,
    )

    assert manifest_path.exists()
    assert receipt_path.exists()
    assert not progress_path.exists()
    assert producer.flush_count == 1
    data = [request for request in producer.requests if request.record_type == "data"]
    terminals = [
        request for request in producer.requests if request.record_type == "terminal"
    ]
    assert len(data) == len(schedule.deliveries)
    assert [r.value for r in data] == [d.payload for d in schedule.deliveries]
    assert len(terminals) == schedule.partition_count
    assert [r.partition for r in terminals] == list(range(schedule.partition_count))
    assert all(
        json.loads(request.value)["replay_manifest_hash"]
        == receipt.replay_manifest_hash
        for request in terminals
    )
    assert receipt.status == "complete"
    assert receipt.acknowledged_delivery_count == len(schedule.deliveries)
    assert set(receipt.acknowledged_delivery_ids) == {
        delivery.delivery_id for delivery in schedule.deliveries
    }
    assert set(receipt.terminal_offsets) == {"0", "1", "2"}
    assert load_replay_manifest(manifest_path) == manifest
    assert load_replay_receipt(receipt_path) == receipt


def test_interrupted_replay_resumes_without_resending_acknowledged_deliveries(
    tmp_path: Path,
) -> None:
    schedule = build_schedule(
        _events(), "resume-fixture", seed=11, partition_count=2, fault="duplicate"
    )
    manifest = make_manifest(schedule, "b" * 64, requested_event_count=3)
    producer = FakeProducer()
    kwargs = {
        "topic": "fixture-events",
        "manifest_path": tmp_path / "replay-manifest.json",
        "progress_path": tmp_path / "progress.json",
        "receipt_path": tmp_path / "receipt.json",
    }

    interrupted = execute_replay(
        schedule, manifest, producer, interrupt_after=2, **kwargs
    )

    assert interrupted is None
    assert not kwargs["receipt_path"].exists()
    progress = load_progress(kwargs["progress_path"])
    assert progress.status == "interrupted"
    assert len(progress.acknowledged_delivery_ids) == 2
    first_ids = [request.delivery_id for request in producer.requests]

    receipt = execute_replay(schedule, manifest, producer, resume=True, **kwargs)

    assert receipt is not None
    data_ids = [
        request.delivery_id
        for request in producer.requests
        if request.record_type == "data"
    ]
    assert data_ids[:2] == first_ids
    assert len(data_ids) == len(set(data_ids)) == len(schedule.deliveries)
    assert receipt.acknowledged_delivery_count == len(schedule.deliveries)
    assert not kwargs["progress_path"].exists()

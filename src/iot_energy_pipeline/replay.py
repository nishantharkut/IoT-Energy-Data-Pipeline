"""Replay manifests, Kafka acknowledgements, and resumable receipt accounting."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from .schedule import ReplaySchedule, max_event_time_lateness


@dataclass(frozen=True)
class ReplayManifest:
    schema_version: str
    replay_run_id: str
    snapshot_hash: str
    schedule_hash: str
    seed: int
    partition_count: int
    fault: str
    requested_event_count: int
    repetition: int
    expected_delivery_count: int
    expected_terminal_count: int
    maximum_event_time_lateness_seconds: float

    def sha256(self) -> str:
        return hashlib.sha256(_canonical_bytes(asdict(self), pretty=False)).hexdigest()


@dataclass(frozen=True)
class DeliveryAcknowledgement:
    delivery_id: str
    partition: int
    offset: int


@dataclass(frozen=True)
class ReplayProgress:
    schema_version: str
    status: str
    replay_run_id: str
    replay_manifest_hash: str
    acknowledged_delivery_ids: tuple[str, ...]
    acknowledgements: tuple[DeliveryAcknowledgement, ...]


@dataclass(frozen=True)
class ReplayReceipt:
    schema_version: str
    status: str
    replay_run_id: str
    replay_manifest_hash: str
    schedule_hash: str
    expected_delivery_count: int
    acknowledged_delivery_count: int
    acknowledged_delivery_ids: tuple[str, ...]
    acknowledgements: tuple[DeliveryAcknowledgement, ...]
    terminal_offsets: dict[str, int]


@dataclass(frozen=True)
class ProduceRequest:
    topic: str
    key: str
    value: str
    partition: int
    headers: tuple[tuple[str, str], ...]
    record_type: str
    delivery_id: str


class ReplayProducer(Protocol):
    def produce(self, request: ProduceRequest) -> int: ...

    def flush(self) -> None: ...


def _canonical_bytes(value: Any, *, pretty: bool) -> bytes:
    if pretty:
        text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    else:
        text = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    return (text + "\n").encode("utf-8")


def _plain(value: Any) -> Any:
    return asdict(cast(Any, value)) if is_dataclass(value) else value


def write_json(value: Any, path: Path) -> None:
    """Atomically write canonical pretty JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_bytes(_plain(value), pretty=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _write_immutable_or_equal(value: Any, path: Path) -> None:
    expected = _canonical_bytes(_plain(value), pretty=True)
    if path.exists():
        if path.read_bytes() != expected:
            raise ValueError(f"immutable replay artifact differs: {path.name}")
        return
    write_json(value, path)


def make_manifest(
    schedule: ReplaySchedule,
    snapshot_hash: str,
    *,
    requested_event_count: int | None = None,
    repetition: int = 1,
) -> ReplayManifest:
    if len(snapshot_hash) != 64 or any(
        character not in "0123456789abcdef" for character in snapshot_hash
    ):
        raise ValueError("snapshot_hash must be a lowercase SHA-256 digest")
    if requested_event_count is None:
        requested_event_count = len(
            {
                delivery.source_event_id
                for delivery in schedule.deliveries
                if delivery.injection_type == "valid"
            }
        )
    if requested_event_count < 0:
        raise ValueError("requested_event_count must be non-negative")
    if repetition < 1:
        raise ValueError("repetition must be positive")
    return ReplayManifest(
        schema_version="replay-manifest-v1",
        replay_run_id=schedule.run_id,
        snapshot_hash=snapshot_hash,
        schedule_hash=schedule.schedule_hash,
        seed=schedule.seed,
        partition_count=schedule.partition_count,
        fault=schedule.fault,
        requested_event_count=requested_event_count,
        repetition=repetition,
        expected_delivery_count=len(schedule.deliveries),
        expected_terminal_count=schedule.partition_count,
        maximum_event_time_lateness_seconds=max_event_time_lateness(schedule),
    )


def snapshot_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_progress(path: Path) -> ReplayProgress:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return ReplayProgress(
        schema_version=raw["schema_version"],
        status=raw["status"],
        replay_run_id=raw["replay_run_id"],
        replay_manifest_hash=raw["replay_manifest_hash"],
        acknowledged_delivery_ids=tuple(raw["acknowledged_delivery_ids"]),
        acknowledgements=tuple(
            DeliveryAcknowledgement(**ack) for ack in raw["acknowledgements"]
        ),
    )


def _load_object(path: Path, artifact: str) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {artifact}: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{artifact} must be a JSON object")
    return raw


def _require_exact_fields(raw: dict[str, Any], model: type[Any], artifact: str) -> None:
    expected = {item.name for item in fields(model)}
    if set(raw) != expected:
        raise ValueError(f"{artifact} fields do not match its schema")


def _require_digest(value: Any, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


def load_replay_manifest(path: Path) -> ReplayManifest:
    """Load a closed replay-manifest-v1 artifact."""

    raw = _load_object(path, "replay manifest")
    _require_exact_fields(raw, ReplayManifest, "replay manifest")
    try:
        manifest = ReplayManifest(**raw)
    except TypeError as exc:
        raise ValueError("invalid replay manifest") from exc
    if manifest.schema_version != "replay-manifest-v1":
        raise ValueError("unsupported replay manifest schema")
    _require_digest(manifest.snapshot_hash, "snapshot_hash")
    _require_digest(manifest.schedule_hash, "schedule_hash")
    if (
        not isinstance(manifest.replay_run_id, str)
        or not manifest.replay_run_id.strip()
    ):
        raise ValueError("replay manifest run ID must be non-empty")
    if manifest.fault not in {
        "clean",
        "duplicate",
        "delayed",
        "malformed",
        "combined",
    }:
        raise ValueError("replay manifest fault is invalid")
    integer_fields = (
        manifest.seed,
        manifest.partition_count,
        manifest.requested_event_count,
        manifest.repetition,
        manifest.expected_delivery_count,
        manifest.expected_terminal_count,
    )
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in integer_fields
    ):
        raise ValueError("replay manifest count fields must be integers")
    if (
        manifest.partition_count < 1
        or manifest.requested_event_count < 0
        or manifest.repetition < 1
        or manifest.expected_delivery_count < 0
        or manifest.expected_terminal_count != manifest.partition_count
    ):
        raise ValueError("replay manifest count fields are invalid")
    lateness = manifest.maximum_event_time_lateness_seconds
    if (
        isinstance(lateness, bool)
        or not isinstance(lateness, (int, float))
        or not math.isfinite(lateness)
        or lateness < 0
    ):
        raise ValueError("replay manifest lateness must be finite and non-negative")
    return manifest


def load_replay_receipt(path: Path) -> ReplayReceipt:
    """Load a complete, closed replay-receipt-v1 artifact."""

    raw = _load_object(path, "replay receipt")
    _require_exact_fields(raw, ReplayReceipt, "replay receipt")
    raw_acknowledgements = raw["acknowledgements"]
    if not isinstance(raw_acknowledgements, list):
        raise ValueError("replay receipt acknowledgements must be a list")
    acknowledgements: list[DeliveryAcknowledgement] = []
    for item in raw_acknowledgements:
        if not isinstance(item, dict):
            raise ValueError("replay receipt acknowledgement must be an object")
        _require_exact_fields(item, DeliveryAcknowledgement, "acknowledgement")
        try:
            acknowledgement = DeliveryAcknowledgement(**item)
        except TypeError as exc:
            raise ValueError("invalid replay acknowledgement") from exc
        if (
            not isinstance(acknowledgement.delivery_id, str)
            or isinstance(acknowledgement.partition, bool)
            or not isinstance(acknowledgement.partition, int)
            or isinstance(acknowledgement.offset, bool)
            or not isinstance(acknowledgement.offset, int)
            or acknowledgement.partition < 0
            or acknowledgement.offset < 0
        ):
            raise ValueError("invalid replay acknowledgement values")
        acknowledgements.append(acknowledgement)
    acknowledged_ids = raw["acknowledged_delivery_ids"]
    terminal_offsets = raw["terminal_offsets"]
    if not isinstance(acknowledged_ids, list) or not all(
        isinstance(delivery_id, str) for delivery_id in acknowledged_ids
    ):
        raise ValueError("replay receipt delivery IDs must be strings")
    if not isinstance(terminal_offsets, dict) or not all(
        isinstance(key, str)
        and isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
        for key, value in terminal_offsets.items()
    ):
        raise ValueError("replay receipt terminal offsets are invalid")
    converted = dict(raw)
    converted["acknowledged_delivery_ids"] = tuple(acknowledged_ids)
    converted["acknowledgements"] = tuple(acknowledgements)
    converted["terminal_offsets"] = terminal_offsets
    try:
        receipt = ReplayReceipt(**converted)
    except TypeError as exc:
        raise ValueError("invalid replay receipt") from exc
    if receipt.schema_version != "replay-receipt-v1" or receipt.status != "complete":
        raise ValueError("replay receipt is not complete replay-receipt-v1")
    _require_digest(receipt.replay_manifest_hash, "replay_manifest_hash")
    _require_digest(receipt.schedule_hash, "schedule_hash")
    if (
        isinstance(receipt.expected_delivery_count, bool)
        or not isinstance(receipt.expected_delivery_count, int)
        or isinstance(receipt.acknowledged_delivery_count, bool)
        or not isinstance(receipt.acknowledged_delivery_count, int)
        or receipt.expected_delivery_count < 0
        or receipt.acknowledged_delivery_count != len(acknowledgements)
        or tuple(ack.delivery_id for ack in acknowledgements)
        != receipt.acknowledged_delivery_ids
    ):
        raise ValueError("replay receipt delivery accounting is invalid")
    return receipt


def _data_request(
    topic: str,
    manifest: ReplayManifest,
    manifest_hash: str,
    delivery: Any,
) -> ProduceRequest:
    return ProduceRequest(
        topic=topic,
        key=delivery.source_event_id,
        value=delivery.payload,
        partition=delivery.partition,
        headers=(
            ("record_type", "data"),
            ("replay_run_id", manifest.replay_run_id),
            ("replay_manifest_hash", manifest_hash),
            ("delivery_id", delivery.delivery_id),
            ("source_event_id", delivery.source_event_id),
            ("injection_type", delivery.injection_type),
            ("payload_sha256", delivery.payload_sha256),
        ),
        record_type="data",
        delivery_id=delivery.delivery_id,
    )


def _terminal_request(
    topic: str,
    manifest: ReplayManifest,
    manifest_hash: str,
    partition: int,
    partition_delivery_count: int,
) -> ProduceRequest:
    terminal_id = hashlib.sha256(
        f"{manifest.replay_run_id}|terminal|{partition}|{manifest_hash}".encode("utf-8")
    ).hexdigest()
    value = json.dumps(
        {
            "record_type": "terminal",
            "replay_run_id": manifest.replay_run_id,
            "replay_manifest_hash": manifest_hash,
            "schedule_hash": manifest.schedule_hash,
            "partition": partition,
            "expected_partition_delivery_count": partition_delivery_count,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return ProduceRequest(
        topic=topic,
        key=f"terminal-{partition}",
        value=value,
        partition=partition,
        headers=(
            ("record_type", "terminal"),
            ("replay_run_id", manifest.replay_run_id),
            ("replay_manifest_hash", manifest_hash),
        ),
        record_type="terminal",
        delivery_id=terminal_id,
    )


def execute_replay(
    schedule: ReplaySchedule,
    manifest: ReplayManifest,
    producer: ReplayProducer,
    *,
    topic: str,
    manifest_path: Path,
    progress_path: Path,
    receipt_path: Path,
    resume: bool = False,
    interrupt_after: int | None = None,
) -> ReplayReceipt | None:
    """Replay and write a receipt only after all data and terminals are acked."""

    if manifest.replay_run_id != schedule.run_id:
        raise ValueError("replay manifest run does not match schedule")
    if manifest.schedule_hash != schedule.schedule_hash:
        raise ValueError("replay manifest schedule hash mismatch")
    if manifest.expected_delivery_count != len(schedule.deliveries):
        raise ValueError("replay manifest delivery count mismatch")
    if not topic.strip():
        raise ValueError("topic must be non-empty")
    if interrupt_after is not None and interrupt_after < 1:
        raise ValueError("interrupt_after must be positive")

    _write_immutable_or_equal(manifest, manifest_path)
    manifest_hash = manifest.sha256()
    acknowledgements: list[DeliveryAcknowledgement] = []
    acknowledged_ids: set[str] = set()
    if resume:
        if not progress_path.exists():
            raise ValueError("resume requested without replay progress")
        progress = load_progress(progress_path)
        if progress.replay_run_id != manifest.replay_run_id:
            raise ValueError("replay progress run mismatch")
        if progress.replay_manifest_hash != manifest_hash:
            raise ValueError("replay progress manifest mismatch")
        acknowledgements.extend(progress.acknowledgements)
        acknowledged_ids.update(progress.acknowledged_delivery_ids)
    elif progress_path.exists():
        raise ValueError("replay progress exists; explicit resume is required")

    expected_ids = {delivery.delivery_id for delivery in schedule.deliveries}
    if not acknowledged_ids <= expected_ids:
        raise ValueError("replay progress contains unknown delivery IDs")

    sent_now = 0
    for delivery in schedule.deliveries:
        if delivery.delivery_id in acknowledged_ids:
            continue
        offset = producer.produce(
            _data_request(topic, manifest, manifest_hash, delivery)
        )
        acknowledgement = DeliveryAcknowledgement(
            delivery.delivery_id, delivery.partition, offset
        )
        acknowledgements.append(acknowledgement)
        acknowledged_ids.add(delivery.delivery_id)
        sent_now += 1
        if interrupt_after is not None and sent_now >= interrupt_after:
            producer.flush()
            progress = ReplayProgress(
                schema_version="replay-progress-v1",
                status="interrupted",
                replay_run_id=manifest.replay_run_id,
                replay_manifest_hash=manifest_hash,
                acknowledged_delivery_ids=tuple(
                    delivery.delivery_id
                    for delivery in schedule.deliveries
                    if delivery.delivery_id in acknowledged_ids
                ),
                acknowledgements=tuple(acknowledgements),
            )
            write_json(progress, progress_path)
            return None

    if acknowledged_ids != expected_ids:
        raise ValueError("not every scheduled delivery was acknowledged")
    per_partition = dict.fromkeys(range(schedule.partition_count), 0)
    position_by_delivery_id: dict[str, int] = {}
    for delivery in schedule.deliveries:
        per_partition[delivery.partition] += 1
        position_by_delivery_id[delivery.delivery_id] = delivery.position
    terminal_offsets: dict[str, int] = {}
    for partition in range(schedule.partition_count):
        request = _terminal_request(
            topic,
            manifest,
            manifest_hash,
            partition,
            per_partition[partition],
        )
        terminal_offsets[str(partition)] = producer.produce(request)
    producer.flush()

    ordered_acknowledgements = tuple(
        sorted(
            acknowledgements,
            key=lambda ack: position_by_delivery_id[ack.delivery_id],
        )
    )
    receipt = ReplayReceipt(
        schema_version="replay-receipt-v1",
        status="complete",
        replay_run_id=manifest.replay_run_id,
        replay_manifest_hash=manifest_hash,
        schedule_hash=manifest.schedule_hash,
        expected_delivery_count=manifest.expected_delivery_count,
        acknowledged_delivery_count=len(ordered_acknowledgements),
        acknowledged_delivery_ids=tuple(
            ack.delivery_id for ack in ordered_acknowledgements
        ),
        acknowledgements=ordered_acknowledgements,
        terminal_offsets=terminal_offsets,
    )
    _write_immutable_or_equal(receipt, receipt_path)
    if progress_path.exists():
        progress_path.unlink()
    return receipt


class ConfluentKafkaProducer:
    """Synchronous acknowledgement adapter over ``confluent-kafka``."""

    def __init__(
        self,
        config: dict[str, Any],
        *,
        acknowledgement_timeout: float = 60.0,
    ):
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("kafka extra is required for Kafka replay") from exc
        self._producer = Producer(config)
        self._timeout = acknowledgement_timeout

    def produce(self, request: ProduceRequest) -> int:
        completed = threading.Event()
        outcome: dict[str, Any] = {}

        def delivered(error: Any, message: Any) -> None:
            outcome["error"] = error
            outcome["message"] = message
            completed.set()

        self._producer.produce(
            request.topic,
            key=request.key,
            value=request.value,
            partition=request.partition,
            headers=list(request.headers),
            on_delivery=delivered,
        )
        deadline = time.monotonic() + self._timeout
        while not completed.wait(0.05):
            self._producer.poll(0)
            if not completed.is_set() and time.monotonic() >= deadline:
                raise TimeoutError("Kafka acknowledgement timed out")
        error = outcome.get("error")
        if error is not None:
            raise RuntimeError(f"Kafka delivery failed: {error}")
        message = outcome.get("message")
        if message is None:
            raise RuntimeError("Kafka acknowledgement did not include a message")
        return cast(int, message.offset())

    def flush(self) -> None:
        remaining = self._producer.flush(self._timeout)
        if remaining:
            raise RuntimeError(f"Kafka flush left {remaining} unacknowledged messages")

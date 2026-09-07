"""Deterministic replay schedules and controlled delivery faults."""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    source_event_id: str
    partition: int
    position: int
    injection_type: str
    payload: str
    payload_sha256: str
    event_time: str


@dataclass(frozen=True)
class ReplaySchedule:
    run_id: str
    seed: int
    partition_count: int
    fault: str
    deliveries: tuple[Delivery, ...]
    delayed_source_event_ids: tuple[str, ...]
    schedule_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seed": self.seed,
            "partition_count": self.partition_count,
            "fault": self.fault,
            "deliveries": [asdict(delivery) for delivery in self.deliveries],
            "delayed_source_event_ids": list(self.delayed_source_event_ids),
            "schedule_hash": self.schedule_hash,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ReplaySchedule:
        """Load a persisted schedule and reject altered or invalid content."""

        required = {
            "run_id",
            "seed",
            "partition_count",
            "fault",
            "deliveries",
            "delayed_source_event_ids",
            "schedule_hash",
        }
        if set(raw) != required:
            raise ValueError("schedule artifact fields do not match schedule-v1")
        run_id = raw["run_id"]
        seed = raw["seed"]
        partition_count = raw["partition_count"]
        fault = raw["fault"]
        schedule_hash = raw["schedule_hash"]
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("schedule run_id must be non-empty")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise ValueError("schedule seed must be an integer")
        partition_for("validation", partition_count)
        if fault not in {"clean", "duplicate", "delayed", "malformed", "combined"}:
            raise ValueError("schedule fault is unsupported")
        if (
            not isinstance(schedule_hash, str)
            or len(schedule_hash) != 64
            or any(character not in "0123456789abcdef" for character in schedule_hash)
        ):
            raise ValueError("schedule hash must be a lowercase SHA-256 digest")
        raw_deliveries = raw["deliveries"]
        if not isinstance(raw_deliveries, list):
            raise ValueError("schedule deliveries must be a list")
        delivery_fields = {
            "delivery_id",
            "source_event_id",
            "partition",
            "position",
            "injection_type",
            "payload",
            "payload_sha256",
            "event_time",
        }
        deliveries: list[Delivery] = []
        permitted_injections = {
            "clean": {"valid"},
            "delayed": {"valid"},
            "duplicate": {"valid", "duplicate"},
            "malformed": {"valid", "malformed"},
            "combined": {"valid", "duplicate", "malformed"},
        }[fault]
        for expected_position, item in enumerate(raw_deliveries):
            if not isinstance(item, dict) or set(item) != delivery_fields:
                raise ValueError("schedule delivery fields do not match delivery-v1")
            try:
                delivery = Delivery(**item)
            except TypeError as exc:
                raise ValueError("invalid schedule delivery") from exc
            if delivery.position != expected_position:
                raise ValueError("schedule delivery positions must be contiguous")
            if delivery.injection_type not in permitted_injections:
                raise ValueError(
                    "schedule delivery injection type conflicts with fault"
                )
            if delivery.partition != partition_for(
                delivery.source_event_id, partition_count
            ):
                raise ValueError("schedule delivery partition is invalid")
            payload_digest = hashlib.sha256(
                delivery.payload.encode("utf-8")
            ).hexdigest()
            if payload_digest != delivery.payload_sha256:
                raise ValueError("schedule delivery payload digest mismatch")
            try:
                datetime.fromisoformat(delivery.event_time.replace("Z", "+00:00"))
            except (AttributeError, ValueError) as exc:
                raise ValueError("schedule delivery event_time is invalid") from exc
            expected_delivery_id = hashlib.sha256(
                (
                    f"{run_id}|{seed}|{delivery.source_event_id}|"
                    f"{delivery.injection_type}|{delivery.position}"
                ).encode("utf-8")
            ).hexdigest()
            if delivery.delivery_id != expected_delivery_id:
                raise ValueError("schedule delivery_id is invalid")
            deliveries.append(delivery)
        if len({delivery.delivery_id for delivery in deliveries}) != len(deliveries):
            raise ValueError("schedule contains duplicate delivery IDs")
        if _digest(asdict(delivery) for delivery in deliveries) != schedule_hash:
            raise ValueError("schedule hash mismatch")
        delayed = raw["delayed_source_event_ids"]
        if not isinstance(delayed, list) or not all(
            isinstance(source_id, str) for source_id in delayed
        ):
            raise ValueError("schedule delayed_source_event_ids must be strings")
        source_ids = {delivery.source_event_id for delivery in deliveries}
        if len(delayed) != len(set(delayed)) or not set(delayed) <= source_ids:
            raise ValueError("schedule delayed source IDs are invalid")
        return cls(
            run_id=run_id,
            seed=seed,
            partition_count=partition_count,
            fault=fault,
            deliveries=tuple(deliveries),
            delayed_source_event_ids=tuple(delayed),
            schedule_hash=schedule_hash,
        )


def partition_for(source_id: str, partition_count: int) -> int:
    if (
        isinstance(partition_count, bool)
        or not isinstance(partition_count, int)
        or partition_count < 1
    ):
        raise ValueError("partition_count must be a positive integer")
    return (
        int.from_bytes(hashlib.sha256(source_id.encode("utf-8")).digest()[:8], "big")
        % partition_count
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(items: Iterable[dict[str, Any]]) -> str:
    return hashlib.sha256(_canonical_json(list(items)).encode("utf-8")).hexdigest()


def _validate_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    source = list(events)
    seen: set[str] = set()
    for event in source:
        source_id = event.get("source_event_id")
        event_time = event.get("event_time_utc")
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("every event requires source_event_id")
        if source_id in seen:
            raise ValueError(f"duplicate canonical source_event_id: {source_id}")
        if not isinstance(event_time, str) or not event_time:
            raise ValueError("every event requires event_time_utc")
        try:
            datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid event_time_utc: {event_time!r}") from exc
        seen.add(source_id)
    return sorted(
        source, key=lambda event: (event["event_time_utc"], event["source_event_id"])
    )


def build_schedule(
    events: Iterable[dict[str, Any]],
    run_id: str,
    seed: int = 0,
    partition_count: int = 3,
    fault: str = "clean",
) -> ReplaySchedule:
    """Resolve a byte-reproducible delivery schedule for one run."""

    if fault not in {"clean", "duplicate", "delayed", "malformed", "combined"}:
        raise ValueError(f"unsupported fault: {fault}")
    if not run_id.strip():
        raise ValueError("run_id must be non-empty")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    partition_for("validation", partition_count)
    source = _validate_events(events)
    delayed_ids: tuple[str, ...] = ()
    if fault in {"delayed", "combined"} and len(source) > 1:
        delayed_index = random.Random(seed).randrange(0, len(source) - 1)
        delayed = source.pop(delayed_index)
        source.append(delayed)
        delayed_ids = (str(delayed["source_event_id"]),)

    pending: list[tuple[dict[str, Any], str, str]] = []
    for event in source:
        payload = _canonical_json(event)
        pending.append((event, "valid", payload))
        if fault in {"duplicate", "combined"}:
            pending.append((event, "duplicate", payload))
        if fault in {"malformed", "combined"}:
            pending.append((event, "malformed", "{not-json"))

    deliveries: list[Delivery] = []
    for position, (event, mode, payload) in enumerate(pending):
        source_id = str(event["source_event_id"])
        delivery_id = hashlib.sha256(
            f"{run_id}|{seed}|{source_id}|{mode}|{position}".encode("utf-8")
        ).hexdigest()
        deliveries.append(
            Delivery(
                delivery_id=delivery_id,
                source_event_id=source_id,
                partition=partition_for(source_id, partition_count),
                position=position,
                injection_type=mode,
                payload=payload,
                payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                event_time=str(event["event_time_utc"]),
            )
        )

    encoded = [asdict(delivery) for delivery in deliveries]
    return ReplaySchedule(
        run_id=run_id,
        seed=seed,
        partition_count=partition_count,
        fault=fault,
        deliveries=tuple(deliveries),
        delayed_source_event_ids=delayed_ids,
        schedule_hash=_digest(encoded),
    )


def max_event_time_lateness(schedule: ReplaySchedule) -> float:
    """Return maximum seconds an event trails the latest prior delivery."""

    latest: datetime | None = None
    maximum = 0.0
    for delivery in schedule.deliveries:
        if delivery.injection_type == "malformed":
            continue
        current = datetime.fromisoformat(delivery.event_time.replace("Z", "+00:00"))
        if latest is not None:
            maximum = max(maximum, (latest - current).total_seconds())
        if latest is None or current > latest:
            latest = current
    return maximum


def load_schedule(path: Path) -> ReplaySchedule:
    """Read a replay schedule artifact with full integrity validation."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read schedule artifact: {path}") from exc
    if not isinstance(raw, dict):
        raise ValueError("schedule artifact must be a JSON object")
    return ReplaySchedule.from_dict(raw)

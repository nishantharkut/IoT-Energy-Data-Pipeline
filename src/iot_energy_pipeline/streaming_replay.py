"""Disk-backed deterministic schedules and acknowledgement ledgers for scale runs."""

from __future__ import annotations

import hashlib
import json
import os
import random
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from iot_energy_pipeline.replay import (
    ProduceRequest,
    ReplayManifest,
    ReplayProducer,
    write_json,
)
from iot_energy_pipeline.schedule import Delivery, partition_for


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _parse_time(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{label} must include a UTC offset")
    return parsed


def _require_digest(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class StreamingScheduleMetadata:
    """Closed metadata for a line-delimited delivery schedule."""

    schema_version: str
    run_id: str
    seed: int
    partition_count: int
    fault: str
    delivery_count: int
    delayed_source_event_ids: tuple[str, ...]
    schedule_sha256: str
    maximum_event_time_lateness_seconds: float
    partition_delivery_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["delayed_source_event_ids"] = list(self.delayed_source_event_ids)
        return value


def _validate_metadata(raw: dict[str, Any]) -> StreamingScheduleMetadata:
    expected = {
        "schema_version",
        "run_id",
        "seed",
        "partition_count",
        "fault",
        "delivery_count",
        "delayed_source_event_ids",
        "schedule_sha256",
        "maximum_event_time_lateness_seconds",
        "partition_delivery_counts",
    }
    if set(raw) != expected:
        raise ValueError("streaming schedule metadata fields are invalid")
    if raw.get("schema_version") != "replay-schedule-v2":
        raise ValueError("unsupported streaming schedule metadata")
    run_id = raw.get("run_id")
    seed = raw.get("seed")
    partition_count = raw.get("partition_count")
    fault = raw.get("fault")
    delivery_count = raw.get("delivery_count")
    delayed = raw.get("delayed_source_event_ids")
    lateness = raw.get("maximum_event_time_lateness_seconds")
    partition_counts = raw.get("partition_delivery_counts")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("streaming schedule run_id is invalid")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("streaming schedule seed is invalid")
    if isinstance(partition_count, bool) or not isinstance(partition_count, int):
        raise ValueError("streaming schedule partition count is invalid")
    partition_for("validation", partition_count)
    if fault not in {"clean", "duplicate", "delayed", "malformed", "combined"}:
        raise ValueError("streaming schedule fault is invalid")
    if (
        isinstance(delivery_count, bool)
        or not isinstance(delivery_count, int)
        or delivery_count < 1
    ):
        raise ValueError("streaming schedule delivery count is invalid")
    if (
        not isinstance(delayed, list)
        or not all(isinstance(source_id, str) for source_id in delayed)
        or len(delayed) != len(set(delayed))
    ):
        raise ValueError("streaming schedule delayed source IDs are invalid")
    if (
        isinstance(lateness, bool)
        or not isinstance(lateness, (int, float))
        or not float(lateness) >= 0
    ):
        raise ValueError("streaming schedule lateness is invalid")
    if (
        not isinstance(partition_counts, dict)
        or set(partition_counts) != {str(value) for value in range(partition_count)}
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in partition_counts.values()
        )
        or sum(partition_counts.values()) != delivery_count
    ):
        raise ValueError("streaming schedule partition counts are invalid")
    return StreamingScheduleMetadata(
        schema_version="replay-schedule-v2",
        run_id=run_id,
        seed=seed,
        partition_count=partition_count,
        fault=fault,
        delivery_count=delivery_count,
        delayed_source_event_ids=tuple(delayed),
        schedule_sha256=_require_digest(raw.get("schedule_sha256"), "schedule_sha256"),
        maximum_event_time_lateness_seconds=float(lateness),
        partition_delivery_counts=dict(partition_counts),
    )


def load_streaming_schedule_metadata(
    path: Path, *, schedule_path: Path | None = None
) -> StreamingScheduleMetadata:
    """Load closed schedule metadata and optionally verify the NDJSON bytes."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cannot read streaming schedule metadata") from exc
    if not isinstance(raw, dict):
        raise ValueError("streaming schedule metadata must be an object")
    metadata = _validate_metadata(raw)
    if schedule_path is not None and (
        not schedule_path.is_file()
        or schedule_path.is_symlink()
        or _sha256(schedule_path) != metadata.schedule_sha256
    ):
        raise ValueError("streaming schedule integrity check failed")
    return metadata


def _delivery(
    *,
    event: dict[str, Any],
    payload: str,
    mode: str,
    run_id: str,
    seed: int,
    partition_count: int,
    position: int,
) -> Delivery:
    source_id = str(event["source_event_id"])
    delivery_id = hashlib.sha256(
        f"{run_id}|{seed}|{source_id}|{mode}|{position}".encode("utf-8")
    ).hexdigest()
    return Delivery(
        delivery_id=delivery_id,
        source_event_id=source_id,
        partition=partition_for(source_id, partition_count),
        position=position,
        injection_type=mode,
        payload=payload,
        payload_sha256=hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        event_time=str(event["event_time_utc"]),
    )


def _ordered_rows(
    connection: sqlite3.Connection,
    *,
    delayed_index: int | None,
) -> Iterator[tuple[str, str, str]]:
    delayed: tuple[str, str, str] | None = None
    cursor = connection.execute(
        "SELECT source_event_id, event_time_utc, payload "
        "FROM events ORDER BY event_time_utc, source_event_id"
    )
    for index, row in enumerate(cursor):
        converted = (str(row[0]), str(row[1]), str(row[2]))
        if delayed_index is not None and index == delayed_index:
            delayed = converted
        else:
            yield converted
    if delayed_index is not None:
        if delayed is None:
            raise ValueError("cannot resolve delayed schedule event")
        yield delayed


def write_streaming_schedule(
    canonical_ndjson: Path,
    output: Path,
    *,
    run_id: str,
    seed: int,
    partition_count: int,
    fault: str,
) -> StreamingScheduleMetadata:
    """External-sort canonical events and write an O(1)-memory schedule ledger."""

    if output.exists():
        raise FileExistsError(f"immutable streaming schedule exists: {output}")
    if not canonical_ndjson.is_file() or canonical_ndjson.is_symlink():
        raise ValueError("canonical NDJSON is unavailable")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be non-empty")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    partition_for("validation", partition_count)
    if fault not in {"clean", "duplicate", "delayed", "malformed", "combined"}:
        raise ValueError(f"unsupported fault: {fault}")

    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, database_name = tempfile.mkstemp(
        prefix=".schedule-sort.", suffix=".sqlite3", dir=output.parent
    )
    os.close(descriptor)
    database_path = Path(database_name)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(
            "CREATE TABLE events ("
            "source_event_id TEXT PRIMARY KEY, "
            "event_time_utc TEXT NOT NULL, payload TEXT NOT NULL"
            ") WITHOUT ROWID"
        )
        count = 0
        previous_source_id: str | None = None
        batch: list[tuple[str, str, str]] = []
        try:
            source = canonical_ndjson.open("r", encoding="utf-8", newline="")
        except (OSError, UnicodeError) as exc:
            raise ValueError("cannot read canonical NDJSON") from exc
        with source:
            for line_number, line in enumerate(source, start=1):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"canonical NDJSON line {line_number} is invalid"
                    ) from exc
                if not isinstance(event, dict):
                    raise ValueError(
                        f"canonical NDJSON line {line_number} is not an object"
                    )
                source_id = _require_digest(
                    event.get("source_event_id"), "canonical source_event_id"
                )
                if previous_source_id is not None and source_id <= previous_source_id:
                    raise ValueError(
                        "canonical NDJSON must be uniquely sorted by source_event_id"
                    )
                event_time = event.get("event_time_utc")
                _parse_time(event_time, "canonical event_time_utc")
                payload = _canonical_json(event)
                batch.append((source_id, str(event_time), payload))
                previous_source_id = source_id
                count += 1
                if len(batch) >= 1000:
                    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", batch)
                    batch.clear()
            if batch:
                connection.executemany("INSERT INTO events VALUES (?, ?, ?)", batch)
        if count < 1:
            raise ValueError("canonical NDJSON contains no events")
        connection.commit()

        delayed_index: int | None = None
        if fault in {"delayed", "combined"} and count > 1:
            delayed_index = random.Random(seed).randrange(0, count - 1)
        delayed_ids: list[str] = []
        partition_counts = {str(value): 0 for value in range(partition_count)}
        position = 0
        latest: datetime | None = None
        maximum_lateness = 0.0
        digest = hashlib.sha256()
        with output.open("xb") as stream:
            for ordered_index, (source_id, event_time, payload) in enumerate(
                _ordered_rows(connection, delayed_index=delayed_index)
            ):
                event = json.loads(payload)
                if delayed_index is not None and ordered_index == count - 1:
                    delayed_ids.append(source_id)
                modes = ["valid"]
                if fault in {"duplicate", "combined"}:
                    modes.append("duplicate")
                if fault in {"malformed", "combined"}:
                    modes.append("malformed")
                for mode in modes:
                    delivery_payload = "{not-json" if mode == "malformed" else payload
                    delivery = _delivery(
                        event=event,
                        payload=delivery_payload,
                        mode=mode,
                        run_id=run_id,
                        seed=seed,
                        partition_count=partition_count,
                        position=position,
                    )
                    encoded = (_canonical_json(asdict(delivery)) + "\n").encode("utf-8")
                    stream.write(encoded)
                    digest.update(encoded)
                    partition_counts[str(delivery.partition)] += 1
                    if mode != "malformed":
                        current = _parse_time(event_time, "schedule event_time")
                        if latest is not None:
                            maximum_lateness = max(
                                maximum_lateness,
                                (latest - current).total_seconds(),
                            )
                        if latest is None or current > latest:
                            latest = current
                    position += 1
            stream.flush()
            os.fsync(stream.fileno())
        return StreamingScheduleMetadata(
            schema_version="replay-schedule-v2",
            run_id=run_id,
            seed=seed,
            partition_count=partition_count,
            fault=fault,
            delivery_count=position,
            delayed_source_event_ids=tuple(delayed_ids),
            schedule_sha256=digest.hexdigest(),
            maximum_event_time_lateness_seconds=maximum_lateness,
            partition_delivery_counts=partition_counts,
        )
    except Exception:
        if output.exists():
            output.unlink()
        raise
    finally:
        connection.close()
        try:
            database_path.unlink()
        except FileNotFoundError:
            pass


def iter_streaming_deliveries(
    path: Path, metadata: StreamingScheduleMetadata
) -> Iterator[Delivery]:
    """Validate and stream a schedule without retaining delivery payloads."""

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
    permitted = {
        "clean": {"valid"},
        "delayed": {"valid"},
        "duplicate": {"valid", "duplicate"},
        "malformed": {"valid", "malformed"},
        "combined": {"valid", "duplicate", "malformed"},
    }[metadata.fault]
    count = 0
    with path.open("r", encoding="utf-8", newline="") as stream:
        for position, line in enumerate(stream):
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"streaming schedule line {position + 1} is invalid"
                ) from exc
            if not isinstance(raw, dict) or set(raw) != delivery_fields:
                raise ValueError("streaming schedule delivery fields are invalid")
            try:
                delivery = Delivery(**raw)
            except TypeError as exc:
                raise ValueError("streaming schedule delivery is invalid") from exc
            if delivery.position != position:
                raise ValueError("streaming schedule positions are not contiguous")
            if delivery.injection_type not in permitted:
                raise ValueError("streaming schedule injection type is invalid")
            if delivery.partition != partition_for(
                delivery.source_event_id, metadata.partition_count
            ):
                raise ValueError("streaming schedule partition is invalid")
            if hashlib.sha256(delivery.payload.encode("utf-8")).hexdigest() != (
                delivery.payload_sha256
            ):
                raise ValueError("streaming schedule payload digest mismatch")
            expected_id = hashlib.sha256(
                (
                    f"{metadata.run_id}|{metadata.seed}|{delivery.source_event_id}|"
                    f"{delivery.injection_type}|{delivery.position}"
                ).encode("utf-8")
            ).hexdigest()
            if delivery.delivery_id != expected_id:
                raise ValueError("streaming schedule delivery ID is invalid")
            _parse_time(delivery.event_time, "streaming schedule event_time")
            count += 1
            yield delivery
    if count != metadata.delivery_count:
        raise ValueError("streaming schedule delivery count mismatch")


def _data_request(
    topic: str,
    manifest: ReplayManifest,
    manifest_hash: str,
    delivery: Delivery,
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
    payload = _canonical_json(
        {
            "record_type": "terminal",
            "replay_run_id": manifest.replay_run_id,
            "replay_manifest_hash": manifest_hash,
            "schedule_hash": manifest.schedule_hash,
            "partition": partition,
            "expected_partition_delivery_count": partition_delivery_count,
        }
    )
    return ProduceRequest(
        topic=topic,
        key=f"terminal-{partition}",
        value=payload,
        partition=partition,
        headers=(
            ("record_type", "terminal"),
            ("replay_run_id", manifest.replay_run_id),
            ("replay_manifest_hash", manifest_hash),
        ),
        record_type="terminal",
        delivery_id=terminal_id,
    )


def _load_progress(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cannot read streaming replay progress") from exc
    expected = {
        "schema_version",
        "status",
        "replay_run_id",
        "replay_manifest_hash",
        "acknowledged_delivery_count",
        "acknowledgement_ledger_bytes",
        "acknowledgement_ledger_sha256",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("streaming replay progress fields are invalid")
    counts = (
        raw.get("acknowledged_delivery_count"),
        raw.get("acknowledgement_ledger_bytes"),
    )
    if (
        raw.get("schema_version") != "replay-progress-v2"
        or raw.get("status") != "interrupted"
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts
        )
    ):
        raise ValueError("streaming replay progress is invalid")
    _require_digest(
        raw.get("acknowledgement_ledger_sha256"),
        "acknowledgement_ledger_sha256",
    )
    return raw


def _validate_acknowledgement(
    raw: object, delivery: Delivery, expected_position: int
) -> None:
    if not isinstance(raw, dict) or set(raw) != {
        "position",
        "delivery_id",
        "partition",
        "offset",
    }:
        raise ValueError("acknowledgement ledger fields are invalid")
    if (
        raw.get("position") != expected_position
        or raw.get("delivery_id") != delivery.delivery_id
        or raw.get("partition") != delivery.partition
        or isinstance(raw.get("offset"), bool)
        or not isinstance(raw.get("offset"), int)
        or raw["offset"] < 0
    ):
        raise ValueError("acknowledgement ledger does not match schedule prefix")


def _write_new_json(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable replay artifact exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def load_streaming_replay_receipt(
    path: Path, *, acknowledgement_path: Path
) -> dict[str, Any]:
    """Load a closed v2 receipt and stream-validate its bound ledger."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("cannot read streaming replay receipt") from exc
    expected = {
        "schema_version",
        "status",
        "replay_run_id",
        "replay_manifest_hash",
        "schedule_hash",
        "expected_delivery_count",
        "acknowledged_delivery_count",
        "acknowledgement_ledger",
        "terminal_offsets",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ValueError("streaming replay receipt fields are invalid")
    expected_count = raw.get("expected_delivery_count")
    acknowledged_count = raw.get("acknowledged_delivery_count")
    descriptor = raw.get("acknowledgement_ledger")
    terminal_offsets = raw.get("terminal_offsets")
    if (
        raw.get("schema_version") != "replay-receipt-v2"
        or raw.get("status") != "complete"
        or not isinstance(raw.get("replay_run_id"), str)
        or not raw["replay_run_id"]
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (expected_count, acknowledged_count)
        )
        or expected_count != acknowledged_count
    ):
        raise ValueError("streaming replay receipt is invalid")
    _require_digest(raw.get("replay_manifest_hash"), "replay_manifest_hash")
    _require_digest(raw.get("schedule_hash"), "schedule_hash")
    if not isinstance(descriptor, dict) or set(descriptor) != {
        "file",
        "record_count",
        "byte_size",
        "sha256",
    }:
        raise ValueError("streaming acknowledgement descriptor is invalid")
    if (
        descriptor.get("file") != acknowledgement_path.name
        or descriptor.get("record_count") != acknowledged_count
        or isinstance(descriptor.get("byte_size"), bool)
        or not isinstance(descriptor.get("byte_size"), int)
        or descriptor["byte_size"] < 0
        or not acknowledgement_path.is_file()
        or acknowledgement_path.is_symlink()
        or acknowledgement_path.stat().st_size != descriptor["byte_size"]
        or _sha256(acknowledgement_path)
        != _require_digest(descriptor.get("sha256"), "acknowledgement sha256")
    ):
        raise ValueError("streaming acknowledgement ledger integrity check failed")
    if not isinstance(terminal_offsets, dict) or not all(
        isinstance(key, str)
        and key.isdigit()
        and isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
        for key, value in terminal_offsets.items()
    ):
        raise ValueError("streaming replay terminal offsets are invalid")

    observed = 0
    with acknowledgement_path.open("r", encoding="utf-8", newline="") as ledger:
        for position, line in enumerate(ledger):
            try:
                acknowledgement = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError("streaming acknowledgement ledger is invalid") from exc
            if not isinstance(acknowledgement, dict) or set(acknowledgement) != {
                "position",
                "delivery_id",
                "partition",
                "offset",
            }:
                raise ValueError("streaming acknowledgement ledger fields are invalid")
            if (
                acknowledgement.get("position") != position
                or isinstance(acknowledgement.get("partition"), bool)
                or not isinstance(acknowledgement.get("partition"), int)
                or acknowledgement["partition"] < 0
                or isinstance(acknowledgement.get("offset"), bool)
                or not isinstance(acknowledgement.get("offset"), int)
                or acknowledgement["offset"] < 0
            ):
                raise ValueError("streaming acknowledgement ledger values are invalid")
            _require_digest(
                acknowledgement.get("delivery_id"), "acknowledgement delivery_id"
            )
            observed += 1
    if observed != acknowledged_count:
        raise ValueError("streaming acknowledgement ledger count mismatch")
    return raw


def execute_streaming_replay(
    *,
    schedule_path: Path,
    metadata: StreamingScheduleMetadata,
    manifest: ReplayManifest,
    producer: ReplayProducer,
    topic: str,
    progress_path: Path,
    acknowledgement_path: Path,
    receipt_path: Path,
    resume: bool = False,
    interrupt_after: int | None = None,
) -> dict[str, Any] | None:
    """Replay a line-delimited schedule with O(1) delivery memory."""

    if receipt_path.exists():
        raise FileExistsError("streaming replay receipt already exists")
    if not topic.strip():
        raise ValueError("topic must be non-empty")
    if interrupt_after is not None and interrupt_after < 1:
        raise ValueError("interrupt_after must be positive")
    if (
        not schedule_path.is_file()
        or schedule_path.is_symlink()
        or _sha256(schedule_path) != metadata.schedule_sha256
    ):
        raise ValueError("streaming schedule integrity check failed")
    manifest_hash = manifest.sha256()
    if (
        manifest.replay_run_id != metadata.run_id
        or manifest.schedule_hash != metadata.schedule_sha256
        or manifest.seed != metadata.seed
        or manifest.partition_count != metadata.partition_count
        or manifest.fault != metadata.fault
        or manifest.expected_delivery_count != metadata.delivery_count
        or manifest.maximum_event_time_lateness_seconds
        != metadata.maximum_event_time_lateness_seconds
    ):
        raise ValueError("streaming schedule does not match replay manifest")

    partial_path = acknowledgement_path.with_name(
        acknowledgement_path.name + ".partial"
    )
    acknowledged_count = 0
    if resume:
        if not progress_path.is_file() or not partial_path.is_file():
            raise ValueError("resume requested without streaming replay progress")
        progress = _load_progress(progress_path)
        acknowledged_count = int(progress["acknowledged_delivery_count"])
        if (
            progress.get("replay_run_id") != manifest.replay_run_id
            or progress.get("replay_manifest_hash") != manifest_hash
            or partial_path.stat().st_size
            != progress.get("acknowledgement_ledger_bytes")
            or _sha256(partial_path) != progress.get("acknowledgement_ledger_sha256")
            or acknowledged_count > metadata.delivery_count
        ):
            raise ValueError("streaming replay progress integrity check failed")
    elif (
        progress_path.exists() or partial_path.exists() or acknowledgement_path.exists()
    ):
        raise ValueError(
            "streaming replay artifacts exist; explicit resume is required"
        )

    deliveries = iter_streaming_deliveries(schedule_path, metadata)
    if acknowledged_count:
        with partial_path.open("r", encoding="utf-8", newline="") as ledger:
            for position in range(acknowledged_count):
                try:
                    delivery = next(deliveries)
                    line = next(ledger)
                except StopIteration as exc:
                    raise ValueError(
                        "streaming replay acknowledgement prefix is incomplete"
                    ) from exc
                try:
                    acknowledgement = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        "streaming replay acknowledgement ledger is invalid"
                    ) from exc
                _validate_acknowledgement(acknowledgement, delivery, position)
            if ledger.readline():
                raise ValueError(
                    "streaming replay acknowledgement ledger has extra records"
                )

    partial_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "ab" if resume else "xb"
    sent_now = 0
    completed_count = acknowledged_count
    with partial_path.open(mode) as ledger:
        for delivery in deliveries:
            offset = producer.produce(
                _data_request(topic, manifest, manifest_hash, delivery)
            )
            acknowledgement = {
                "position": delivery.position,
                "delivery_id": delivery.delivery_id,
                "partition": delivery.partition,
                "offset": offset,
            }
            ledger.write((_canonical_json(acknowledgement) + "\n").encode("utf-8"))
            completed_count += 1
            sent_now += 1
            if interrupt_after is not None and sent_now >= interrupt_after:
                producer.flush()
                ledger.flush()
                os.fsync(ledger.fileno())
                write_json(
                    {
                        "schema_version": "replay-progress-v2",
                        "status": "interrupted",
                        "replay_run_id": manifest.replay_run_id,
                        "replay_manifest_hash": manifest_hash,
                        "acknowledged_delivery_count": completed_count,
                        "acknowledgement_ledger_bytes": partial_path.stat().st_size,
                        "acknowledgement_ledger_sha256": _sha256(partial_path),
                    },
                    progress_path,
                )
                return None
        ledger.flush()
        os.fsync(ledger.fileno())
    if completed_count != metadata.delivery_count:
        raise ValueError("not every streaming schedule delivery was acknowledged")

    terminal_offsets: dict[str, int] = {}
    for partition in range(metadata.partition_count):
        request = _terminal_request(
            topic,
            manifest,
            manifest_hash,
            partition,
            metadata.partition_delivery_counts[str(partition)],
        )
        terminal_offsets[str(partition)] = producer.produce(request)
    producer.flush()
    if acknowledgement_path.exists():
        raise FileExistsError("immutable acknowledgement ledger already exists")
    partial_path.replace(acknowledgement_path)
    receipt: dict[str, Any] = {
        "schema_version": "replay-receipt-v2",
        "status": "complete",
        "replay_run_id": manifest.replay_run_id,
        "replay_manifest_hash": manifest_hash,
        "schedule_hash": manifest.schedule_hash,
        "expected_delivery_count": manifest.expected_delivery_count,
        "acknowledged_delivery_count": completed_count,
        "acknowledgement_ledger": {
            "file": acknowledgement_path.name,
            "record_count": completed_count,
            "byte_size": acknowledgement_path.stat().st_size,
            "sha256": _sha256(acknowledgement_path),
        },
        "terminal_offsets": terminal_offsets,
    }
    _write_new_json(receipt_path, receipt)
    if progress_path.exists():
        progress_path.unlink()
    return receipt

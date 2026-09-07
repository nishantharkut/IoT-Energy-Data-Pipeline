"""Exact Gold finalization from replay-complete Bronze data."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from iot_energy_pipeline.replay import ReplayManifest, ReplayReceipt
from iot_energy_pipeline.schedule import ReplaySchedule

from .aggregates import aggregate


@dataclass(frozen=True)
class FinalizedGold:
    source_events: tuple[dict[str, Any], ...]
    aggregates: tuple[dict[str, Any], ...]
    manifest: dict[str, Any]


def _canonical_line(record: dict[str, Any]) -> bytes:
    return (
        json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def _ndjson_bytes(records: Iterable[dict[str, Any]]) -> bytes:
    return b"".join(_canonical_line(record) for record in records)


def _pretty_json_bytes(record: dict[str, Any]) -> bytes:
    return (
        json.dumps(record, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _headers(record: dict[str, Any]) -> dict[str, str]:
    raw = record.get("headers", {})
    if isinstance(raw, dict):
        return {str(key): str(value) for key, value in raw.items()}
    result: dict[str, str] = {}
    for header in raw:
        if isinstance(header, dict):
            result[str(header["key"])] = str(header["value"])
        else:
            key, value = header
            result[str(key)] = str(value)
    return result


def _delivery_id(record: dict[str, Any]) -> str:
    value = record.get("delivery_id") or _headers(record).get("delivery_id")
    if not isinstance(value, str) or not value:
        raise ValueError("Bronze record is missing delivery_id")
    return value


def _raw_value(record: dict[str, Any]) -> str:
    value = record.get("value", record.get("payload"))
    if not isinstance(value, str):
        raise ValueError("Bronze record is missing raw Kafka value")
    return value


def _validate_replay_accounting(
    schedule: ReplaySchedule,
    manifest: ReplayManifest,
    receipt: ReplayReceipt,
    bronze: list[dict[str, Any]],
    terminals: list[dict[str, Any]],
) -> dict[str, Any]:
    if schedule.run_id != manifest.replay_run_id:
        raise ValueError("schedule and replay manifest run mismatch")
    if schedule.schedule_hash != manifest.schedule_hash:
        raise ValueError("schedule hash mismatch")
    manifest_hash = manifest.sha256()
    if receipt.replay_manifest_hash != manifest_hash:
        raise ValueError("receipt replay manifest hash mismatch")
    if receipt.schedule_hash != schedule.schedule_hash:
        raise ValueError("receipt schedule hash mismatch")

    expected_ids = [delivery.delivery_id for delivery in schedule.deliveries]
    if len(expected_ids) != len(set(expected_ids)):
        raise ValueError("schedule contains duplicate delivery IDs")
    receipt_ids = list(receipt.acknowledged_delivery_ids)
    if (
        receipt.acknowledged_delivery_count != len(receipt_ids)
        or len(receipt_ids) != len(set(receipt_ids))
        or set(receipt_ids) != set(expected_ids)
    ):
        raise ValueError("receipt delivery coverage mismatch")
    if {ack.delivery_id for ack in receipt.acknowledgements} != set(expected_ids):
        raise ValueError("receipt acknowledgement details mismatch")

    bronze_ids = [_delivery_id(record) for record in bronze]
    if len(bronze_ids) != len(set(bronze_ids)):
        raise ValueError("duplicate Bronze delivery_id")
    if set(bronze_ids) != set(expected_ids):
        raise ValueError("Bronze delivery coverage mismatch")
    if len(bronze) != manifest.expected_delivery_count:
        raise ValueError("Bronze delivery count mismatch")

    offsets = [(int(record["partition"]), int(record["offset"])) for record in bronze]
    if len(offsets) != len(set(offsets)):
        raise ValueError("duplicate Bronze partition/offset")
    bronze_by_id = {_delivery_id(record): record for record in bronze}
    schedule_by_id = {
        delivery.delivery_id: delivery for delivery in schedule.deliveries
    }
    receipt_by_id = {ack.delivery_id: ack for ack in receipt.acknowledgements}
    for delivery_id, record in bronze_by_id.items():
        scheduled = schedule_by_id[delivery_id]
        acknowledged = receipt_by_id[delivery_id]
        location = (int(record["partition"]), int(record["offset"]))
        if location != (scheduled.partition, acknowledged.offset):
            raise ValueError(f"Bronze acknowledgement mismatch for {delivery_id}")
        header = _headers(record)
        if header.get("replay_manifest_hash") != manifest_hash:
            raise ValueError("Bronze run-manifest binding mismatch")

    expected_partitions = set(range(schedule.partition_count))
    if set(receipt.terminal_offsets) != {str(p) for p in expected_partitions}:
        raise ValueError("receipt terminal partition coverage mismatch")
    terminal_partitions = [int(record["partition"]) for record in terminals]
    if (
        len(terminal_partitions) != len(set(terminal_partitions))
        or set(terminal_partitions) != expected_partitions
    ):
        raise ValueError("terminal partition coverage mismatch")
    for record in terminals:
        partition = int(record["partition"])
        if int(record["offset"]) != receipt.terminal_offsets[str(partition)]:
            raise ValueError("terminal offset mismatch")
        try:
            payload = json.loads(_raw_value(record))
        except json.JSONDecodeError as exc:
            raise ValueError("terminal payload is invalid JSON") from exc
        if (
            payload.get("record_type") != "terminal"
            or payload.get("partition") != partition
            or payload.get("replay_manifest_hash") != manifest_hash
            or payload.get("schedule_hash") != schedule.schedule_hash
        ):
            raise ValueError("terminal payload contract mismatch")
    return {
        "manifest_hash": manifest_hash,
        "bronze_by_id": bronze_by_id,
        "schedule_by_id": schedule_by_id,
    }


def finalize_run(
    canonical: list[dict[str, Any]],
    schedule: ReplaySchedule,
    replay_manifest: ReplayManifest,
    receipt: ReplayReceipt,
    bronze: list[dict[str, Any]],
    terminal_records: list[dict[str, Any]],
    output_dir: Path | None = None,
) -> FinalizedGold:
    """Validate replay completion and rebuild exact records from Bronze."""

    if output_dir is not None and output_dir.exists():
        raise FileExistsError(f"immutable Gold output already exists: {output_dir}")
    canonical_ids = [str(event["source_event_id"]) for event in canonical]
    if len(canonical_ids) != len(set(canonical_ids)):
        raise ValueError("canonical snapshot has duplicate source_event_id")
    canonical_by_id = {str(event["source_event_id"]): event for event in canonical}
    canonical_digest = {
        source_id: _sha256_bytes(_canonical_line(event).rstrip(b"\n"))
        for source_id, event in canonical_by_id.items()
    }
    accounting = _validate_replay_accounting(
        schedule,
        replay_manifest,
        receipt,
        bronze,
        terminal_records,
    )
    schedule_by_id = accounting["schedule_by_id"]

    candidates: dict[str, list[tuple[tuple[int, int], str, dict[str, Any]]]] = {}
    invalid_delivery_count = 0
    for record in bronze:
        delivery_id = _delivery_id(record)
        scheduled = schedule_by_id[delivery_id]
        raw = _raw_value(record)
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as exc:
            if scheduled.injection_type != "malformed":
                raise ValueError(
                    f"scheduled valid delivery is malformed: {delivery_id}"
                ) from exc
            if (
                hashlib.sha256(raw.encode("utf-8")).hexdigest()
                != scheduled.payload_sha256
            ):
                raise ValueError(
                    f"scheduled payload digest mismatch: {delivery_id}"
                ) from exc
            invalid_delivery_count += 1
            continue
        if not isinstance(event, dict):
            raise ValueError(f"valid JSON delivery is not an object: {delivery_id}")
        source_id = event.get("source_event_id")
        if source_id not in canonical_by_id:
            raise ValueError(f"unknown source_event_id in Bronze: {source_id!r}")
        raw_digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        if raw_digest != scheduled.payload_sha256:
            raise ValueError(
                "canonical payload digest mismatch for scheduled delivery "
                f"{delivery_id}"
            )
        if raw_digest != canonical_digest[str(source_id)]:
            raise ValueError(
                f"canonical payload digest mismatch for source_event_id {source_id}"
            )
        location = (int(record["partition"]), int(record["offset"]))
        candidates.setdefault(str(source_id), []).append((location, delivery_id, event))

    if set(candidates) != set(canonical_by_id):
        missing = sorted(set(canonical_by_id) - set(candidates))
        raise ValueError(f"incomplete source coverage: {missing[:5]}")
    selected_events: list[dict[str, Any]] = []
    selected_deliveries: dict[str, dict[str, Any]] = {}
    for source_id in sorted(candidates):
        location, delivery_id, event = min(
            candidates[source_id], key=lambda item: item[0]
        )
        selected_events.append(event)
        selected_deliveries[source_id] = {
            "delivery_id": delivery_id,
            "partition": location[0],
            "offset": location[1],
        }
    gold_aggregates = aggregate(selected_events)
    source_payload = _ndjson_bytes(selected_events)
    aggregate_payload = _ndjson_bytes(gold_aggregates)
    receipt_hash = _sha256_bytes(_pretty_json_bytes(asdict(receipt)))
    gold_manifest: dict[str, Any] = {
        "schema_version": "gold-manifest-v1",
        "status": "gold_finalized",
        "replay_run_id": replay_manifest.replay_run_id,
        "replay_manifest_hash": accounting["manifest_hash"],
        "replay_receipt_sha256": receipt_hash,
        "schedule_hash": schedule.schedule_hash,
        "bronze_delivery_count": len(bronze),
        "terminal_record_count": len(terminal_records),
        "valid_delivery_count": sum(len(value) for value in candidates.values()),
        "invalid_delivery_count": invalid_delivery_count,
        "duplicate_valid_delivery_count": sum(
            max(0, len(value) - 1) for value in candidates.values()
        ),
        "source_coverage_count": len(selected_events),
        "selected_deliveries": selected_deliveries,
        "artifacts": {
            "gold_source_events": {
                "file": "gold-source-events.ndjson",
                "record_count": len(selected_events),
                "sha256": _sha256_bytes(source_payload),
            },
            "gold_aggregates": {
                "file": "gold-aggregates.ndjson",
                "record_count": len(gold_aggregates),
                "sha256": _sha256_bytes(aggregate_payload),
            },
        },
    }
    if output_dir is not None:
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        stage = Path(
            tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
        )
        try:
            (stage / "gold-source-events.ndjson").write_bytes(source_payload)
            (stage / "gold-aggregates.ndjson").write_bytes(aggregate_payload)
            (stage / "gold-manifest.json").write_bytes(
                _pretty_json_bytes(gold_manifest)
            )
            if output_dir.exists():
                raise FileExistsError(
                    f"immutable Gold output already exists: {output_dir}"
                )
            stage.replace(output_dir)
        except Exception:
            if stage.exists():
                shutil.rmtree(stage)
            raise
    return FinalizedGold(tuple(selected_events), tuple(gold_aggregates), gold_manifest)


def finalize_gold(
    bronze: list[dict[str, Any]],
    canonical: list[dict[str, Any]],
    expected_delivery_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Backward-compatible local aggregate helper for legacy unit callers."""

    if expected_delivery_ids is not None:
        actual_ids = [_delivery_id(record) for record in bronze]
        if (
            len(actual_ids) != len(set(actual_ids))
            or set(actual_ids) != expected_delivery_ids
        ):
            raise ValueError("Bronze delivery coverage does not match replay schedule")
    canonical_ids = {event["source_event_id"] for event in canonical}
    selected: dict[str, dict[str, Any]] = {}
    for delivery in bronze:
        if delivery.get("error"):
            continue
        event = delivery.get("event", delivery)
        source_id = event.get("source_event_id")
        if source_id in canonical_ids and source_id not in selected:
            selected[source_id] = event
    if set(selected) != canonical_ids:
        raise ValueError("incomplete source coverage")
    return aggregate(list(selected.values()))


def write_gold(records: list[dict[str, Any]], path: Path) -> None:
    if path.exists():
        raise FileExistsError("immutable Gold output already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_ndjson_bytes(records))

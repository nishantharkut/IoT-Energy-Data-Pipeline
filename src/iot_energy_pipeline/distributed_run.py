"""Prepare deterministic distributed runs from an immutable canonical snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any

from iot_energy_pipeline.distributed_fixture import (
    _ensure_topic,
    _topic_for,
    replay_distributed_fixture,
)
from iot_energy_pipeline.replay import (
    ConfluentKafkaProducer,
    ReplayManifest,
    load_replay_manifest,
    write_json,
)
from iot_energy_pipeline.streaming_replay import (
    execute_streaming_replay,
    load_streaming_schedule_metadata,
    write_streaming_schedule,
)


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


def _artifact_path(snapshot_dir: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("snapshot integrity descriptor has an unsafe path")
    posix = PurePosixPath(relative)
    if posix.is_absolute() or any(part in {"", ".", ".."} for part in posix.parts):
        raise ValueError("snapshot integrity descriptor has an unsafe path")
    path = snapshot_dir.joinpath(*posix.parts)
    try:
        path.resolve(strict=True).relative_to(snapshot_dir.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise ValueError("snapshot integrity descriptor has an unsafe path") from exc
    return path


def _select_canonical_prefix(
    source: Path,
    destination: Path,
    *,
    expected_count: int,
    selected_count: int,
) -> None:
    """Validate the complete source while retaining only the selected prefix."""

    observed_count = 0
    previous_source_id: str | None = None
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        source_stream = source.open("r", encoding="utf-8", newline="")
    except (OSError, UnicodeError) as exc:
        raise ValueError("cannot read canonical snapshot") from exc
    with source_stream, destination.open("xb") as output:
        for line_number, line in enumerate(source_stream, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"canonical snapshot line {line_number} is invalid JSON"
                ) from exc
            if not isinstance(record, dict):
                raise ValueError(
                    f"canonical snapshot line {line_number} is not an object"
                )
            source_id = record.get("source_event_id")
            if (
                not isinstance(source_id, str)
                or len(source_id) != 64
                or any(character not in "0123456789abcdef" for character in source_id)
                or (previous_source_id is not None and source_id <= previous_source_id)
            ):
                raise ValueError(
                    "canonical snapshot must be uniquely sorted by source_event_id"
                )
            if observed_count < selected_count:
                payload = json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
                output.write((payload + "\n").encode("utf-8"))
            previous_source_id = source_id
            observed_count += 1
    if observed_count != expected_count:
        raise ValueError("canonical snapshot record count mismatch")


def prepare_distributed_run(
    *,
    snapshot_dir: Path,
    output_dir: Path,
    run_id: str,
    fault: str,
    event_count: int,
    seed: int = 20260825,
    partition_count: int = 3,
    watermark: str = "24 hours",
    max_offsets_per_trigger: int = 10000,
) -> dict[str, Any]:
    """Prepare an immutable schedule and selected canonical input; run no services."""

    if output_dir.exists():
        raise FileExistsError(f"distributed run already exists: {output_dir}")
    if (
        isinstance(event_count, bool)
        or not isinstance(event_count, int)
        or event_count < 1
    ):
        raise ValueError("event_count must be a positive integer")
    if (
        isinstance(partition_count, bool)
        or not isinstance(partition_count, int)
        or partition_count < 1
    ):
        raise ValueError("partition_count must be a positive integer")
    if (
        isinstance(max_offsets_per_trigger, bool)
        or not isinstance(max_offsets_per_trigger, int)
        or max_offsets_per_trigger < 1
    ):
        raise ValueError("max_offsets_per_trigger must be a positive integer")
    manifest_path = snapshot_dir / "snapshot-manifest.json"
    snapshot_manifest = _json_object(manifest_path, "snapshot manifest")
    if (
        snapshot_manifest.get("schema_version") != "snapshot-manifest-v1"
        or snapshot_manifest.get("status") != "complete"
    ):
        raise ValueError("snapshot manifest is not complete")
    artifacts = snapshot_manifest.get("artifacts")
    descriptor = (
        artifacts.get("canonical_ndjson") if isinstance(artifacts, dict) else None
    )
    if not isinstance(descriptor, dict):
        raise ValueError("snapshot integrity descriptor is invalid")
    full_count = descriptor.get("record_count")
    if (
        isinstance(full_count, bool)
        or not isinstance(full_count, int)
        or full_count < 1
    ):
        raise ValueError("snapshot integrity record count is invalid")
    canonical_path = _artifact_path(snapshot_dir, descriptor.get("file"))
    if canonical_path.is_symlink() or _sha256(canonical_path) != descriptor.get(
        "sha256"
    ):
        raise ValueError("snapshot integrity check failed")
    if event_count > full_count:
        raise ValueError("event_count exceeds the canonical snapshot")

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        selected_path = stage / "snapshot/canonical.ndjson"
        _select_canonical_prefix(
            canonical_path,
            selected_path,
            expected_count=full_count,
            selected_count=event_count,
        )
        shutil.copyfile(manifest_path, stage / "snapshot/source-snapshot-manifest.json")
        replay_dir = stage / "replay"
        replay_dir.mkdir()
        schedule_path = replay_dir / "replay-schedule.ndjson"
        schedule_metadata = write_streaming_schedule(
            selected_path,
            schedule_path,
            run_id=run_id,
            seed=seed,
            partition_count=partition_count,
            fault=fault,
        )
        schedule_metadata_path = replay_dir / "replay-schedule-metadata.json"
        write_json(schedule_metadata.to_dict(), schedule_metadata_path)
        replay_manifest = ReplayManifest(
            schema_version="replay-manifest-v1",
            replay_run_id=run_id,
            snapshot_hash=_sha256(selected_path),
            schedule_hash=schedule_metadata.schedule_sha256,
            seed=seed,
            partition_count=partition_count,
            fault=fault,
            requested_event_count=event_count,
            repetition=1,
            expected_delivery_count=schedule_metadata.delivery_count,
            expected_terminal_count=partition_count,
            maximum_event_time_lateness_seconds=(
                schedule_metadata.maximum_event_time_lateness_seconds
            ),
        )
        replay_manifest_path = replay_dir / "replay-manifest.json"
        write_json(replay_manifest, replay_manifest_path)
        plan: dict[str, Any] = {
            "schema_version": "distributed-run-plan-v2",
            "status": "prepared",
            "run_id": run_id,
            "topic": _topic_for(run_id),
            "fault": fault,
            "seed": seed,
            "partition_count": partition_count,
            "watermark": watermark,
            "max_offsets_per_trigger": max_offsets_per_trigger,
            "selection": "source_event_id_sorted_prefix",
            "canonical_event_count": event_count,
            "full_snapshot_event_count": full_count,
            "source_snapshot_sha256": _sha256(canonical_path),
            "source_snapshot_manifest_sha256": _sha256(manifest_path),
            "canonical_ndjson_path": "snapshot/canonical.ndjson",
            "canonical_ndjson_sha256": _sha256(selected_path),
            "schedule_format": "delivery-ndjson-v2",
            "schedule_path": "replay/replay-schedule.ndjson",
            "schedule_metadata_path": "replay/replay-schedule-metadata.json",
            "schedule_metadata_sha256": _sha256(schedule_metadata_path),
            "schedule_hash": schedule_metadata.schedule_sha256,
            "replay_manifest_path": "replay/replay-manifest.json",
            "replay_manifest_hash": replay_manifest.sha256(),
            "expected_delivery_count": replay_manifest.expected_delivery_count,
            "expected_terminal_count": replay_manifest.expected_terminal_count,
        }
        write_json(plan, stage / "distributed-plan.json")
        if output_dir.exists():
            raise FileExistsError(f"distributed run already exists: {output_dir}")
        stage.replace(output_dir)
        return plan
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def replay_distributed_run(
    run_dir: Path,
    *,
    bootstrap_servers: str,
    resume: bool = False,
    interrupt_after: int | None = None,
) -> Any | None:
    """Replay either a scale ledger or the compatible bounded fixture format."""

    plan = _json_object(run_dir / "distributed-plan.json", "distributed run plan")
    if plan.get("schedule_format") != "delivery-ndjson-v2":
        return replay_distributed_fixture(
            run_dir,
            bootstrap_servers=bootstrap_servers,
            resume=resume,
            interrupt_after=interrupt_after,
        )
    if (
        plan.get("schema_version") != "distributed-run-plan-v2"
        or plan.get("status") != "prepared"
    ):
        raise ValueError("distributed scale run is not prepared")
    schedule_path = run_dir / str(plan.get("schedule_path"))
    metadata_path = run_dir / str(plan.get("schedule_metadata_path"))
    manifest_path = run_dir / str(plan.get("replay_manifest_path"))
    if _sha256(metadata_path) != plan.get("schedule_metadata_sha256"):
        raise ValueError("distributed plan schedule metadata binding mismatch")
    metadata = load_streaming_schedule_metadata(
        metadata_path, schedule_path=schedule_path
    )
    manifest = load_replay_manifest(manifest_path)
    canonical_path = run_dir / str(plan.get("canonical_ndjson_path"))
    if (
        metadata.schedule_sha256 != plan.get("schedule_hash")
        or manifest.sha256() != plan.get("replay_manifest_hash")
        or _sha256(canonical_path) != manifest.snapshot_hash
    ):
        raise ValueError("distributed scale replay artifact binding mismatch")
    _ensure_topic(bootstrap_servers, str(plan.get("topic")), metadata.partition_count)
    producer = ConfluentKafkaProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": f"iot-energy-{metadata.run_id}",
        }
    )
    replay_root = run_dir / "replay"
    return execute_streaming_replay(
        schedule_path=schedule_path,
        metadata=metadata,
        manifest=manifest,
        producer=producer,
        topic=str(plan.get("topic")),
        progress_path=replay_root / "replay-progress.json",
        acknowledgement_path=replay_root / "replay-acknowledgements.ndjson",
        receipt_path=replay_root / "replay-receipt.json",
        resume=resume,
        interrupt_after=interrupt_after,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--snapshot-dir", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument(
        "--fault",
        choices=("clean", "duplicate", "delayed", "malformed", "combined"),
        required=True,
    )
    prepare.add_argument("--event-count", type=int, required=True)
    prepare.add_argument("--seed", type=int, default=20260825)
    prepare.add_argument("--partitions", type=int, default=3)
    prepare.add_argument("--watermark", default="24 hours")
    prepare.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    replay = commands.add_parser("replay")
    replay.add_argument("--run-dir", type=Path, required=True)
    replay.add_argument("--bootstrap-servers", required=True)
    replay.add_argument("--resume", action="store_true")
    replay.add_argument("--interrupt-after", type=int)
    args = parser.parse_args()
    if args.command == "prepare":
        plan = prepare_distributed_run(
            snapshot_dir=args.snapshot_dir,
            output_dir=args.output,
            run_id=args.run_id,
            fault=args.fault,
            event_count=args.event_count,
            seed=args.seed,
            partition_count=args.partitions,
            watermark=args.watermark,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        print(json.dumps(plan, sort_keys=True))
        return 0
    receipt = replay_distributed_run(
        args.run_dir,
        bootstrap_servers=args.bootstrap_servers,
        resume=args.resume,
        interrupt_after=args.interrupt_after,
    )
    print("interrupted" if receipt is None else "complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

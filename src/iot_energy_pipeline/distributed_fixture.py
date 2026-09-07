"""Prepare and replay the coherent fixture through a real Kafka broker."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .canonicalize import build_snapshot
from .fixture import _create_fixture_source, _fixture_rules
from .manifests import register_dataset
from .profile import profile_dataset
from .replay import (
    ConfluentKafkaProducer,
    ReplayReceipt,
    execute_replay,
    load_replay_manifest,
    make_manifest,
    write_json,
)
from .schedule import build_schedule, load_schedule


def _topic_for(run_id: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._-]+", "-", run_id).strip("-.")
    if not normalized:
        raise ValueError("run_id does not contain a Kafka topic-safe character")
    return f"iot-energy-{normalized}"[:249]


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid canonical NDJSON line {line_number}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"canonical NDJSON line {line_number} is not an object")
        records.append(value)
    return records


def prepare_distributed_fixture(
    output_dir: Path,
    *,
    run_id: str,
    fault: str,
    seed: int = 20260825,
    partition_count: int = 3,
    watermark: str = "1 minute",
    max_offsets_per_trigger: int = 60,
) -> dict[str, Any]:
    """Create immutable source, snapshot, schedule, and replay-plan artifacts."""

    if max_offsets_per_trigger < 1:
        raise ValueError("max_offsets_per_trigger must be positive")
    if output_dir.exists():
        raise FileExistsError(f"distributed fixture already exists: {output_dir}")
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
            dataset_manifest,
            stage / "snapshot",
            _fixture_rules(),
        )
        canonical_path = stage / "snapshot/canonical.ndjson"
        canonical = _read_ndjson(canonical_path)
        schedule = build_schedule(
            canonical,
            run_id,
            seed=seed,
            partition_count=partition_count,
            fault=fault,
        )
        replay_root = stage / "replay"
        schedule_path = replay_root / "replay-schedule.json"
        write_json(schedule.to_dict(), schedule_path)
        manifest = make_manifest(
            schedule,
            snapshot.manifest["artifacts"]["canonical_ndjson"]["sha256"],
            requested_event_count=len(canonical),
        )
        manifest_path = replay_root / "replay-manifest.json"
        write_json(manifest, manifest_path)
        plan: dict[str, Any] = {
            "schema_version": "distributed-fixture-plan-v1",
            "status": "prepared",
            "run_id": run_id,
            "topic": _topic_for(run_id),
            "fault": fault,
            "seed": seed,
            "partition_count": partition_count,
            "watermark": watermark,
            "max_offsets_per_trigger": max_offsets_per_trigger,
            "canonical_event_count": len(canonical),
            "canonical_ndjson_path": "snapshot/canonical.ndjson",
            "canonical_ndjson_sha256": manifest.snapshot_hash,
            "canonical_parquet_path": "snapshot/canonical.parquet",
            "schedule_path": "replay/replay-schedule.json",
            "schedule_hash": schedule.schedule_hash,
            "replay_manifest_path": "replay/replay-manifest.json",
            "replay_manifest_hash": manifest.sha256(),
            "expected_delivery_count": manifest.expected_delivery_count,
            "expected_terminal_count": manifest.expected_terminal_count,
        }
        write_json(plan, stage / "distributed-plan.json")
        if output_dir.exists():
            raise FileExistsError(f"distributed fixture already exists: {output_dir}")
        stage.replace(output_dir)
        return plan
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def _load_plan(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "distributed-plan.json"
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read distributed fixture plan: {path}") from exc
    if not isinstance(plan, dict) or plan.get("status") != "prepared":
        raise ValueError("distributed fixture is not in prepared state")
    return plan


def _ensure_topic(
    bootstrap_servers: str,
    topic: str,
    partition_count: int,
) -> None:
    try:
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("kafka extra is required for distributed replay") from exc
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(topic=topic, timeout=30)
    existing = metadata.topics.get(topic)
    if existing is not None and existing.error is None:
        if len(existing.partitions) != partition_count:
            raise ValueError("existing Kafka topic partition count mismatch")
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partition_count, replication_factor=1)]
    )
    try:
        futures[topic].result(timeout=30)
    except KafkaException as exc:
        error = exc.args[0] if exc.args else None
        if error is not None and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            metadata = admin.list_topics(topic=topic, timeout=30)
            existing = metadata.topics.get(topic)
            if existing is not None and existing.error is None:
                if len(existing.partitions) != partition_count:
                    raise ValueError(
                        "existing Kafka topic partition count mismatch"
                    ) from exc
                return
        raise RuntimeError(f"cannot create Kafka topic {topic}: {exc}") from exc


def replay_distributed_fixture(
    run_dir: Path,
    *,
    bootstrap_servers: str,
    resume: bool = False,
    interrupt_after: int | None = None,
) -> ReplayReceipt | None:
    """Execute the persisted schedule against Kafka and save broker receipts."""

    plan = _load_plan(run_dir)
    canonical_path = run_dir / str(plan["canonical_ndjson_path"])
    schedule = load_schedule(run_dir / str(plan["schedule_path"]))
    manifest_path = run_dir / str(plan["replay_manifest_path"])
    manifest = load_replay_manifest(manifest_path)
    if schedule.schedule_hash != plan.get("schedule_hash"):
        raise ValueError("distributed plan schedule hash mismatch")
    if manifest.sha256() != plan.get("replay_manifest_hash"):
        raise ValueError("distributed plan replay manifest hash mismatch")
    if (
        hashlib.sha256(canonical_path.read_bytes()).hexdigest()
        != manifest.snapshot_hash
    ):
        raise ValueError("distributed plan canonical snapshot hash mismatch")
    replay_root = run_dir / "replay"
    receipt_path = replay_root / "replay-receipt.json"
    if receipt_path.exists():
        raise FileExistsError("distributed replay receipt already exists")
    _ensure_topic(
        bootstrap_servers,
        str(plan["topic"]),
        schedule.partition_count,
    )
    producer = ConfluentKafkaProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": f"iot-energy-{schedule.run_id}",
        }
    )
    return execute_replay(
        schedule,
        manifest,
        producer,
        topic=str(plan["topic"]),
        manifest_path=manifest_path,
        progress_path=replay_root / "replay-progress.json",
        receipt_path=receipt_path,
        resume=resume,
        interrupt_after=interrupt_after,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--run-id", required=True)
    prepare.add_argument(
        "--fault",
        choices=("clean", "duplicate", "delayed", "malformed", "combined"),
        default="combined",
    )
    prepare.add_argument("--seed", type=int, default=20260825)
    prepare.add_argument("--partitions", type=int, default=3)
    prepare.add_argument("--watermark", default="1 minute")
    prepare.add_argument("--max-offsets-per-trigger", type=int, default=60)
    replay = subparsers.add_parser("replay")
    replay.add_argument("--run-dir", type=Path, required=True)
    replay.add_argument("--bootstrap-servers", required=True)
    replay.add_argument("--resume", action="store_true")
    replay.add_argument("--interrupt-after", type=int)
    args = parser.parse_args()
    if args.command == "prepare":
        plan = prepare_distributed_fixture(
            args.output,
            run_id=args.run_id,
            fault=args.fault,
            seed=args.seed,
            partition_count=args.partitions,
            watermark=args.watermark,
            max_offsets_per_trigger=args.max_offsets_per_trigger,
        )
        print(json.dumps(plan, sort_keys=True))
        return
    receipt = replay_distributed_fixture(
        args.run_dir,
        bootstrap_servers=args.bootstrap_servers,
        resume=args.resume,
        interrupt_after=args.interrupt_after,
    )
    print("interrupted" if receipt is None else "complete")


if __name__ == "__main__":
    main()

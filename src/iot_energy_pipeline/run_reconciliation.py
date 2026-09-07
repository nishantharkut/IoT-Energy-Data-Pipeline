"""Fail-closed binding of Spark Gold, Hadoop oracles, and layer accounting."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any, cast

from jobs.hadoop.reconcile import reconcile_sorted_ndjson


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


def _relative_file(root: Path, relative: object, label: str) -> Path:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError(f"unsafe {label} path")
    posix = PurePosixPath(relative)
    if posix.is_absolute() or any(part in {"", ".", ".."} for part in posix.parts):
        raise ValueError(f"unsafe {label} path")
    target = root.joinpath(*posix.parts)
    try:
        target.resolve(strict=False).relative_to(root.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise ValueError(f"unsafe {label} path") from exc
    return target


def _verify_artifact(
    root: Path,
    descriptor: object,
    *,
    path_field: str,
    label: str,
) -> Path:
    if not isinstance(descriptor, dict):
        raise ValueError(f"{label} artifact integrity descriptor is invalid")
    path = _relative_file(root, descriptor.get(path_field), label)
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{label} artifact integrity check failed")
    if path.stat().st_size != descriptor.get("byte_size"):
        raise ValueError(f"{label} artifact integrity check failed")
    if _sha256(path) != descriptor.get("sha256"):
        raise ValueError(f"{label} artifact integrity check failed")
    count = descriptor.get("record_count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"{label} artifact record count is invalid")
    return path


def _require_equal(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise ValueError(f"{label} mismatch")


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} is invalid")
    return value


def _write_atomic_new(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable reconciliation already exists: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if path.exists():
            raise FileExistsError(
                f"immutable reconciliation already exists: {path.name}"
            )
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def reconcile_distributed_run(
    *,
    run_dir: Path,
    oracle_dir: Path,
    layer_audit_dir: Path,
    output: Path,
) -> dict[str, Any]:
    """Verify all finalized artifacts and compare both independent oracles."""

    started = time.perf_counter()
    if output.exists():
        raise FileExistsError(f"immutable reconciliation already exists: {output.name}")
    plan = _json_object(run_dir / "distributed-plan.json", "distributed plan")
    gold = _json_object(run_dir / "gold/gold-manifest.json", "Gold manifest")
    oracle = _json_object(
        oracle_dir / "hadoop-oracle-manifest.json", "Hadoop oracle manifest"
    )
    layers = _json_object(
        layer_audit_dir / "layer-audit-manifest.json", "layer audit manifest"
    )
    if plan.get("status") != "prepared":
        raise ValueError("distributed plan is not prepared")
    if (
        gold.get("schema_version") != "distributed-gold-manifest-v1"
        or gold.get("status") != "gold_finalized"
    ):
        raise ValueError("Gold manifest is not finalized")
    if (
        oracle.get("schema_version") != "hadoop-oracle-manifest-v2"
        or oracle.get("status") != "complete"
    ):
        raise ValueError("Hadoop oracle manifest is not complete")
    if (
        layers.get("schema_version") != "layer-audit-manifest-v1"
        or layers.get("status") != "complete"
    ):
        raise ValueError("layer audit manifest is not complete")

    replay_hash = plan.get("replay_manifest_hash")
    run_id = plan.get("run_id")
    _require_equal(gold.get("replay_run_id"), run_id, "Gold run ID")
    _require_equal(oracle.get("run_id"), run_id, "Hadoop run ID")
    _require_equal(gold.get("replay_manifest_hash"), replay_hash, "Gold replay hash")
    _require_equal(
        layers.get("replay_manifest_hash"), replay_hash, "layer audit replay hash"
    )

    canonical = _relative_file(
        run_dir, plan.get("canonical_ndjson_path"), "canonical NDJSON"
    )
    if not canonical.is_file() or canonical.is_symlink():
        raise ValueError("canonical NDJSON is missing")
    oracle_input = oracle.get("input")
    if not isinstance(oracle_input, dict):
        raise ValueError("Hadoop oracle input binding is invalid")
    _require_equal(
        oracle_input.get("local_sha256"),
        _sha256(canonical),
        "Hadoop canonical input hash",
    )

    gold_artifacts = gold.get("artifacts")
    oracle_outputs = oracle.get("outputs")
    layer_artifacts = layers.get("artifacts")
    if not all(
        isinstance(value, dict)
        for value in (gold_artifacts, oracle_outputs, layer_artifacts)
    ):
        raise ValueError("one or more artifact registries are invalid")
    gold_artifacts = cast(dict[str, Any], gold_artifacts)
    oracle_outputs = cast(dict[str, Any], oracle_outputs)
    layer_artifacts = cast(dict[str, Any], layer_artifacts)
    gold_source_descriptor = gold_artifacts.get("gold_source_events_ndjson")
    gold_aggregate_descriptor = gold_artifacts.get("gold_aggregates_ndjson")
    oracle_source_descriptor = oracle_outputs.get("source_records")
    oracle_aggregate_descriptor = oracle_outputs.get("aggregates")
    descriptors = (
        gold_source_descriptor,
        gold_aggregate_descriptor,
        oracle_source_descriptor,
        oracle_aggregate_descriptor,
    )
    if not all(isinstance(descriptor, dict) for descriptor in descriptors):
        raise ValueError("one or more reconciliation artifact descriptors are invalid")
    gold_source_descriptor = cast(dict[str, Any], gold_source_descriptor)
    gold_aggregate_descriptor = cast(dict[str, Any], gold_aggregate_descriptor)
    oracle_source_descriptor = cast(dict[str, Any], oracle_source_descriptor)
    oracle_aggregate_descriptor = cast(dict[str, Any], oracle_aggregate_descriptor)
    gold_source_path = _verify_artifact(
        run_dir / "gold",
        gold_source_descriptor,
        path_field="file",
        label="Gold source records",
    )
    gold_aggregate_path = _verify_artifact(
        run_dir / "gold",
        gold_aggregate_descriptor,
        path_field="file",
        label="Gold aggregates",
    )
    oracle_source_path = _verify_artifact(
        oracle_dir,
        oracle_source_descriptor,
        path_field="local_file",
        label="Hadoop source records",
    )
    oracle_aggregate_path = _verify_artifact(
        oracle_dir,
        oracle_aggregate_descriptor,
        path_field="local_file",
        label="Hadoop aggregates",
    )
    _verify_artifact(
        layer_audit_dir,
        layer_artifacts.get("layer_dispositions_ndjson"),
        path_field="file",
        label="layer dispositions",
    )
    candidate_descriptor = layer_artifacts.get("silver_candidate_events_ndjson")
    if not isinstance(candidate_descriptor, dict):
        raise ValueError("Silver candidate artifact descriptor is invalid")
    candidate_path = _verify_artifact(
        layer_audit_dir,
        candidate_descriptor,
        path_field="file",
        label="Silver candidate events",
    )

    expected_source_count = _nonnegative_int(
        plan.get("canonical_event_count"), "canonical event count"
    )
    expected_delivery_count = _nonnegative_int(
        plan.get("expected_delivery_count"), "expected delivery count"
    )
    expected_terminal_count = _nonnegative_int(
        plan.get("expected_terminal_count"), "expected terminal count"
    )
    _require_equal(
        gold.get("source_coverage_count"), expected_source_count, "Gold source count"
    )
    _require_equal(
        gold.get("bronze_delivery_count"),
        expected_delivery_count,
        "Gold Bronze count",
    )
    _require_equal(
        gold.get("terminal_record_count"),
        expected_terminal_count,
        "Gold terminal count",
    )
    layer_counts = layers.get("counts")
    if not isinstance(layer_counts, dict):
        raise ValueError("layer counts are invalid")
    count_fields = (
        "bronze_deliveries",
        "parsed_valid_deliveries",
        "quarantine_deliveries",
        "silver_deliveries",
        "duplicate_valid_deliveries",
        "late_valid_deliveries",
        "terminal_records",
    )
    checked_counts = {
        field: _nonnegative_int(layer_counts.get(field), f"layer count {field}")
        for field in count_fields
    }
    _require_equal(
        layer_counts.get("bronze_deliveries"),
        expected_delivery_count,
        "layer Bronze count",
    )
    _require_equal(
        layer_counts.get("terminal_records"),
        expected_terminal_count,
        "layer terminal count",
    )
    bronze_count = checked_counts["bronze_deliveries"]
    parsed_count = checked_counts["parsed_valid_deliveries"]
    quarantine_count = checked_counts["quarantine_deliveries"]
    dispositions = sum(
        checked_counts[field]
        for field in (
            "silver_deliveries",
            "duplicate_valid_deliveries",
            "late_valid_deliveries",
            "quarantine_deliveries",
        )
    )
    if parsed_count + quarantine_count != bronze_count or dispositions != bronze_count:
        raise ValueError("layer conservation mismatch")
    _require_equal(
        candidate_descriptor.get("record_count"),
        layer_counts.get("silver_deliveries"),
        "Silver candidate artifact count",
    )

    source_comparison = reconcile_sorted_ndjson(
        gold_source_path,
        oracle_source_path,
        key_field="source_event_id",
        record_label="source record",
        expected_count=gold_source_descriptor["record_count"],
        actual_count=oracle_source_descriptor["record_count"],
    )
    aggregate_comparison = reconcile_sorted_ndjson(
        gold_aggregate_path,
        oracle_aggregate_path,
        key_field="group_key",
        record_label="group",
        expected_count=gold_aggregate_descriptor["record_count"],
        actual_count=oracle_aggregate_descriptor["record_count"],
    )
    _require_equal(
        source_comparison["expected_record_count"],
        expected_source_count,
        "Gold source coverage",
    )
    _require_equal(
        source_comparison["actual_record_count"],
        expected_source_count,
        "Hadoop source coverage",
    )
    source_errors = source_comparison["errors"]
    aggregate_errors = aggregate_comparison["errors"]
    report: dict[str, Any] = {
        "schema_version": "reconciliation-v2",
        "status": "passed" if not source_errors and not aggregate_errors else "failed",
        "run_id": run_id,
        "replay_manifest_hash": replay_hash,
        "layer_counts": layer_counts,
        "aggregate_comparison": {
            "spark_record_count": aggregate_comparison["expected_record_count"],
            "hadoop_record_count": aggregate_comparison["actual_record_count"],
            "exact_fields": aggregate_comparison["exact_fields"],
            "errors": aggregate_errors,
        },
        "source_record_comparison": {
            "spark_record_count": source_comparison["expected_record_count"],
            "hadoop_record_count": source_comparison["actual_record_count"],
            "exact_fields": source_comparison["exact_fields"],
            "errors": source_errors,
        },
        "bindings": {
            "canonical_ndjson_sha256": _sha256(canonical),
            "gold_manifest_sha256": _sha256(run_dir / "gold/gold-manifest.json"),
            "hadoop_manifest_sha256": _sha256(
                oracle_dir / "hadoop-oracle-manifest.json"
            ),
            "layer_audit_manifest_sha256": _sha256(
                layer_audit_dir / "layer-audit-manifest.json"
            ),
            "silver_candidate_events_sha256": _sha256(candidate_path),
        },
        "reconciliation_seconds": time.perf_counter() - started,
    }
    _write_atomic_new(output, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--oracle-dir", type=Path, required=True)
    parser.add_argument("--layer-audit-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = reconcile_distributed_run(
        run_dir=args.run_dir,
        oracle_dir=args.oracle_dir,
        layer_audit_dir=args.layer_audit_dir,
        output=args.output,
    )
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())

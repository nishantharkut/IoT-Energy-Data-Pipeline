"""Build a decision-impact report from already finalized distributed artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, cast

from iot_energy_pipeline.reports import build_decision_impact_report_from_ndjson


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


def _artifact(
    root: Path,
    descriptor: object,
    *,
    path_field: str,
    label: str,
) -> tuple[Path, int, int]:
    if not isinstance(descriptor, dict):
        raise ValueError(f"{label} artifact descriptor is invalid")
    path = _relative_file(root, descriptor.get(path_field), label)
    count = descriptor.get("record_count")
    byte_size = descriptor.get("byte_size")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"{label} record count is invalid")
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
        raise ValueError(f"{label} byte size is invalid")
    if (
        not path.is_file()
        or path.is_symlink()
        or path.stat().st_size != byte_size
        or _sha256(path) != descriptor.get("sha256")
    ):
        raise ValueError(f"{label} artifact integrity check failed")
    return path, count, byte_size


def _seconds(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite non-negative number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{label} must be a finite non-negative number")
    return result


def _require_hash_binding(
    bindings: dict[str, Any], field: str, path: Path, label: str
) -> None:
    if bindings.get(field) != _sha256(path):
        raise ValueError(f"{label} binding mismatch")


def _write_atomic_new(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"immutable decision-impact report exists: {path.name}")
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
                f"immutable decision-impact report exists: {path.name}"
            )
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def build_distributed_business_report(
    *,
    run_dir: Path,
    oracle_dir: Path,
    layer_audit_dir: Path,
    reconciliation_path: Path,
    output: Path,
) -> dict[str, Any]:
    """Verify finalized artifacts and write a hypothetical impact report."""

    if output.exists():
        raise FileExistsError(f"immutable decision-impact report exists: {output.name}")
    gold = _json_object(run_dir / "gold/gold-manifest.json", "Gold manifest")
    oracle = _json_object(
        oracle_dir / "hadoop-oracle-manifest.json", "Hadoop oracle manifest"
    )
    layers = _json_object(
        layer_audit_dir / "layer-audit-manifest.json", "layer audit manifest"
    )
    reconciliation = _json_object(reconciliation_path, "reconciliation report")
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
    if (
        reconciliation.get("schema_version") != "reconciliation-v2"
        or reconciliation.get("status") != "passed"
    ):
        raise ValueError("decision impact requires a passed reconciliation-v2 report")
    run_id = gold.get("replay_run_id")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("Gold run ID is invalid")
    if reconciliation.get("run_id") != run_id:
        raise ValueError("reconciliation run ID mismatch")
    if oracle.get("run_id") != run_id:
        raise ValueError("Hadoop run ID mismatch")
    replay_hash = gold.get("replay_manifest_hash")
    if (
        not isinstance(replay_hash, str)
        or len(replay_hash) != 64
        or layers.get("replay_manifest_hash") != replay_hash
        or reconciliation.get("replay_manifest_hash") != replay_hash
    ):
        raise ValueError("replay manifest binding mismatch")
    bindings = reconciliation.get("bindings")
    if not isinstance(bindings, dict):
        raise ValueError("reconciliation bindings are invalid")
    _require_hash_binding(
        bindings,
        "gold_manifest_sha256",
        run_dir / "gold/gold-manifest.json",
        "Gold manifest",
    )
    _require_hash_binding(
        bindings,
        "hadoop_manifest_sha256",
        oracle_dir / "hadoop-oracle-manifest.json",
        "Hadoop manifest",
    )
    _require_hash_binding(
        bindings,
        "layer_audit_manifest_sha256",
        layer_audit_dir / "layer-audit-manifest.json",
        "layer audit manifest",
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

    gold_source, gold_count, gold_source_bytes = _artifact(
        run_dir / "gold",
        gold_artifacts.get("gold_source_events_ndjson"),
        path_field="file",
        label="Gold source events",
    )
    _, _, gold_aggregate_bytes = _artifact(
        run_dir / "gold",
        gold_artifacts.get("gold_aggregates_ndjson"),
        path_field="file",
        label="Gold aggregates",
    )
    _, _, oracle_source_bytes = _artifact(
        oracle_dir,
        oracle_outputs.get("source_records"),
        path_field="local_file",
        label="Hadoop source records",
    )
    _, _, oracle_aggregate_bytes = _artifact(
        oracle_dir,
        oracle_outputs.get("aggregates"),
        path_field="local_file",
        label="Hadoop aggregates",
    )
    candidate, candidate_count, candidate_bytes = _artifact(
        layer_audit_dir,
        layer_artifacts.get("silver_candidate_events_ndjson"),
        path_field="file",
        label="Silver candidate events",
    )
    _require_hash_binding(
        bindings,
        "silver_candidate_events_sha256",
        candidate,
        "Silver candidate events",
    )
    _, _, disposition_bytes = _artifact(
        layer_audit_dir,
        layer_artifacts.get("layer_dispositions_ndjson"),
        path_field="file",
        label="layer dispositions",
    )

    timings = {
        "gold_finalization_seconds": _seconds(
            gold.get("gold_finalization_seconds"), "Gold finalization time"
        ),
        "hadoop_oracle_seconds": _seconds(
            oracle.get("verification_seconds"), "Hadoop oracle time"
        ),
        "layer_audit_seconds": _seconds(
            layers.get("layer_audit_seconds"), "layer audit time"
        ),
        "reconciliation_seconds": _seconds(
            reconciliation.get("reconciliation_seconds"), "reconciliation time"
        ),
    }
    verification_seconds = sum(timings.values())
    verification_storage_scope: dict[str, int | list[str]] = {
        "gold_source_events_bytes": gold_source_bytes,
        "gold_aggregates_bytes": gold_aggregate_bytes,
        "hadoop_source_records_bytes": oracle_source_bytes,
        "hadoop_aggregates_bytes": oracle_aggregate_bytes,
        "layer_dispositions_bytes": disposition_bytes,
        "reconciliation_report_bytes": reconciliation_path.stat().st_size,
        "excludes": [
            "Silver candidate events (reported as candidate storage)",
            "Bronze/Silver/Quarantine runtime storage outside finalized artifacts",
        ],
    }
    verification_storage_bytes = sum(
        cast(int, verification_storage_scope[key])
        for key in (
            "gold_source_events_bytes",
            "gold_aggregates_bytes",
            "hadoop_source_records_bytes",
            "hadoop_aggregates_bytes",
            "layer_dispositions_bytes",
            "reconciliation_report_bytes",
        )
    )
    report = build_decision_impact_report_from_ndjson(
        candidate,
        gold_source,
        candidate_record_count=candidate_count,
        verified_record_count=gold_count,
        scratch_directory=output.parent,
        reconciliation_passed=True,
        verification_seconds=verification_seconds,
        candidate_storage_bytes=candidate_bytes,
        verification_storage_bytes=verification_storage_bytes,
    )
    report.update(
        {
            "run_id": run_id,
            "verification_time_components": timings,
            "verification_storage_scope": verification_storage_scope,
            "reconciliation_sha256": _sha256(reconciliation_path),
        }
    )
    _write_atomic_new(output, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--oracle-dir", type=Path, required=True)
    parser.add_argument("--layer-audit-dir", type=Path, required=True)
    parser.add_argument("--reconciliation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build_distributed_business_report(
        run_dir=args.run_dir,
        oracle_dir=args.oracle_dir,
        layer_audit_dir=args.layer_audit_dir,
        reconciliation_path=args.reconciliation,
        output=args.output,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

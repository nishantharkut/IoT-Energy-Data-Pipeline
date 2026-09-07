"""Seal a self-contained, reconciled distributed run for read-only publication."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

from iot_energy_pipeline.machine_metadata import validate_machine_metadata

QUERY_NAMES = ("meter", "building", "time_window", "quality", "exposure")
STORAGE_FORMATS = (
    "ndjson",
    "parquet_zstd_unpartitioned",
    "parquet_zstd_partitioned",
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


def _inside(root: Path, path: Path, label: str, *, directory: bool = False) -> Path:
    try:
        resolved_root = root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        resolved.relative_to(resolved_root)
    except (OSError, ValueError) as exc:
        raise ValueError(f"{label} must be inside the run directory") from exc
    if (
        path.is_symlink()
        or (directory and not path.is_dir())
        or (not directory and not path.is_file())
    ):
        raise ValueError(f"{label} must be inside the run directory")
    return path


def _require_hash(actual: object, path: Path, label: str) -> None:
    if actual != _sha256(path):
        raise ValueError(f"{label} binding mismatch")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_nonnegative_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_integer(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 1


def _valid_summary(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "median",
        "minimum",
        "maximum",
    }:
        return False
    median = value["median"]
    minimum = value["minimum"]
    maximum = value["maximum"]
    return (
        _is_nonnegative_number(median)
        and _is_nonnegative_number(minimum)
        and _is_nonnegative_number(maximum)
        and minimum <= median <= maximum
    )


def _validate_query_benchmark(
    benchmark: dict[str, Any],
    *,
    run_id: str,
    gold_manifest_path: Path,
    decision_report_path: Path,
) -> None:
    if set(benchmark) != {
        "schema_version",
        "status",
        "run_id",
        "gold_manifest_sha256",
        "decision_impact_sha256",
        "parameters",
        "results",
    }:
        raise ValueError("query benchmark has an incomplete or open contract")
    parameters = benchmark.get("parameters")
    if (
        not isinstance(parameters, dict)
        or set(parameters)
        != {"meter_id", "building_id", "window_start_utc", "window_end_utc"}
        or not isinstance(parameters.get("meter_id"), str)
        or not parameters["meter_id"]
        or not isinstance(parameters.get("building_id"), str)
        or not isinstance(parameters.get("window_start_utc"), str)
        or not parameters["window_start_utc"]
        or not isinstance(parameters.get("window_end_utc"), str)
        or not parameters["window_end_utc"]
    ):
        raise ValueError("query benchmark parameters are invalid")
    results = benchmark.get("results")
    if not isinstance(results, list) or len(results) != len(QUERY_NAMES):
        raise ValueError("query benchmark results are incomplete")
    for expected_name, result in zip(QUERY_NAMES, results, strict=True):
        if (
            not isinstance(result, dict)
            or set(result)
            != {"query", "repetitions", "row_count", "seconds", "sql_sha256"}
            or result.get("query") != expected_name
            or result.get("repetitions") != 3
            or isinstance(result.get("row_count"), bool)
            or not isinstance(result.get("row_count"), int)
            or result["row_count"] < 0
            or not _valid_summary(result.get("seconds"))
            or not _is_sha256(result.get("sql_sha256"))
        ):
            raise ValueError(f"query benchmark result {expected_name!r} is invalid")
    if (
        benchmark.get("schema_version") != "query-benchmark-v1"
        or benchmark.get("status") != "complete"
        or benchmark.get("run_id") != run_id
        or benchmark.get("gold_manifest_sha256") != _sha256(gold_manifest_path)
        or benchmark.get("decision_impact_sha256") != _sha256(decision_report_path)
    ):
        raise ValueError("query benchmark is not complete or bound to this run")


def _validate_storage_benchmark(
    benchmark: dict[str, Any], *, run_id: str, plan: dict[str, Any]
) -> None:
    if set(benchmark) != {
        "schema_version",
        "status",
        "run_id",
        "record_count",
        "formats",
    }:
        raise ValueError("storage benchmark has an incomplete or open contract")
    expected_record_count = plan.get("canonical_event_count")
    record_count = benchmark.get("record_count")
    formats = benchmark.get("formats")
    if (
        benchmark.get("schema_version") != "storage-benchmark-v1"
        or benchmark.get("status") != "complete"
        or benchmark.get("run_id") != run_id
        or not _is_positive_integer(record_count)
        or not _is_positive_integer(expected_record_count)
        or record_count != expected_record_count
        or not isinstance(formats, dict)
        or set(formats) != set(STORAGE_FORMATS)
    ):
        raise ValueError("storage benchmark is not complete or bound to this run")
    ndjson = formats["ndjson"]
    unpartitioned = formats["parquet_zstd_unpartitioned"]
    partitioned = formats["parquet_zstd_partitioned"]
    if (
        not isinstance(ndjson, dict)
        or set(ndjson)
        != {
            "byte_size",
            "file_count",
            "sha256",
            "write_seconds",
            "relative_to_ndjson",
        }
        or not _is_positive_integer(ndjson.get("byte_size"))
        or ndjson.get("file_count") != 1
        or ndjson.get("sha256") != plan.get("canonical_ndjson_sha256")
        or ndjson.get("write_seconds") is not None
        or ndjson.get("relative_to_ndjson") != 1.0
    ):
        raise ValueError("storage benchmark NDJSON descriptor is invalid")
    ndjson_bytes = ndjson["byte_size"]
    descriptors = (
        (unpartitioned, False, "unpartitioned"),
        (partitioned, True, "partitioned"),
    )
    for descriptor, is_partitioned, label in descriptors:
        expected_keys = {
            "byte_size",
            "file_count",
            "write_seconds",
            "relative_to_ndjson",
        }
        if is_partitioned:
            expected_keys.add("partition_columns")
        if (
            not isinstance(descriptor, dict)
            or set(descriptor) != expected_keys
            or not _is_positive_integer(descriptor.get("byte_size"))
            or not _is_positive_integer(descriptor.get("file_count"))
            or not _is_nonnegative_number(descriptor.get("write_seconds"))
            or not _is_nonnegative_number(descriptor.get("relative_to_ndjson"))
            or descriptor["relative_to_ndjson"] <= 0
            or not math.isclose(
                descriptor["relative_to_ndjson"],
                descriptor["byte_size"] / ndjson_bytes,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
            or (
                is_partitioned
                and descriptor.get("partition_columns")
                != ["source_variant", "meter_id"]
            )
        ):
            raise ValueError(f"storage benchmark {label} descriptor is invalid")


def _artifact_registry(run_dir: Path) -> dict[str, dict[str, Any]]:
    registry: dict[str, dict[str, Any]] = {}
    for path in sorted(
        run_dir.rglob("*"), key=lambda item: item.relative_to(run_dir).as_posix()
    ):
        if path.is_symlink():
            raise ValueError("sealed runs cannot contain symbolic links")
        if not path.is_file():
            continue
        relative = path.relative_to(run_dir).as_posix()
        if relative in {"run-manifest.json", "run-manifest.sha256"}:
            continue
        name = relative.replace("/", "__").replace(".", "_")
        registry[name] = {
            "relative_path": relative,
            "byte_size": path.stat().st_size,
            "sha256": _sha256(path),
        }
    if not registry:
        raise ValueError("cannot seal a run with no artifacts")
    return registry


def _views(
    plan: dict[str, Any],
    gold: dict[str, Any],
    layers: dict[str, Any],
    reconciliation: dict[str, Any],
    decision: dict[str, Any],
    machine: dict[str, Any],
    query_benchmark: dict[str, Any] | None,
    storage_benchmark: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    dispositions = layers.get("disposition_counts")
    if not isinstance(dispositions, dict):
        raise ValueError("layer disposition counts are invalid")
    disposition_rows = [
        {"disposition": name, "delivery_count": count}
        for name, count in sorted(dispositions.items())
    ]
    return {
        "provenance_status": {
            "title": "Provenance and status",
            "rows": [
                {
                    "run_id": plan["run_id"],
                    "status": "finalized",
                    "fault": plan.get("fault"),
                    "seed": plan.get("seed"),
                    "watermark": plan.get("watermark"),
                    "replay_manifest_sha256": plan["replay_manifest_hash"],
                    "canonical_ndjson_sha256": reconciliation.get("bindings", {}).get(
                        "canonical_ndjson_sha256"
                    ),
                    "execution_context": machine["execution_context"],
                    "hostname": machine["hostname"],
                }
            ],
        },
        "throughput_layers": {
            "title": "Throughput and layer conservation",
            "rows": [
                {
                    **layers["counts"],
                    "expected_deliveries": plan.get("expected_delivery_count"),
                    "expected_terminal_records": plan.get("expected_terminal_count"),
                    "gold_source_events": gold.get("source_coverage_count"),
                }
            ],
        },
        "fault_disposition": {
            "title": "Fault disposition",
            "rows": disposition_rows,
        },
        "energy_quality": {
            "title": "Verified energy and decision impact",
            "rows": [
                {
                    "affected_meter_periods": decision.get(
                        "affected_meter_period_count"
                    ),
                    "absolute_kwh_discrepancy": decision.get(
                        "absolute_kwh_discrepancy"
                    ),
                    "candidate_flagged_records": decision.get(
                        "candidate_flagged_record_count"
                    ),
                    "verified_flagged_records": decision.get(
                        "verified_flagged_record_count"
                    ),
                    "tariff_scenarios": decision.get("tariff_scenarios"),
                    "monetary_interpretation": (
                        "hypothetical scenarios only; not HKUST financial results"
                    ),
                }
            ],
        },
        "storage_queries": {
            "title": "Verification time and storage",
            "rows": [
                {
                    "verification_seconds": decision.get("verification_seconds"),
                    "candidate_storage_bytes": decision.get("candidate_storage_bytes"),
                    "verification_storage_bytes": decision.get(
                        "verification_storage_bytes"
                    ),
                    "storage_overhead_ratio": decision.get("storage_overhead_ratio"),
                    "query_results": (
                        []
                        if query_benchmark is None
                        else query_benchmark.get("results", [])
                    ),
                    "storage_formats": (
                        {}
                        if storage_benchmark is None
                        else storage_benchmark.get("formats", {})
                    ),
                }
            ],
        },
        "reconciliation": {
            "title": "Spark-Hadoop exact reconciliation",
            "rows": [reconciliation],
        },
    }


def _write_seal(run_dir: Path, manifest: dict[str, Any]) -> None:
    manifest_path = run_dir / "run-manifest.json"
    seal_path = run_dir / "run-manifest.sha256"
    payload = (
        json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    created_manifest = False
    try:
        with manifest_path.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        created_manifest = True
        with seal_path.open("x", encoding="ascii", newline="\n") as stream:
            stream.write(hashlib.sha256(payload).hexdigest() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        if created_manifest and not seal_path.exists():
            try:
                manifest_path.unlink()
            except OSError:
                pass
        raise


def seal_distributed_run(
    *,
    run_dir: Path,
    oracle_dir: Path,
    layer_audit_dir: Path,
    reconciliation_path: Path,
    decision_report_path: Path,
    machine_metadata_path: Path,
) -> dict[str, Any]:
    """Seal a verified run without launching or modifying any data-processing job."""

    if (run_dir / "run-manifest.json").exists() or (
        run_dir / "run-manifest.sha256"
    ).exists():
        raise FileExistsError("run is already sealed")
    _inside(run_dir, oracle_dir, "Hadoop oracle directory", directory=True)
    _inside(run_dir, layer_audit_dir, "layer audit directory", directory=True)
    _inside(run_dir, reconciliation_path, "reconciliation report")
    _inside(run_dir, decision_report_path, "decision-impact report")
    _inside(run_dir, machine_metadata_path, "machine metadata")

    plan_path = _inside(run_dir, run_dir / "distributed-plan.json", "run plan")
    gold_path = _inside(run_dir, run_dir / "gold/gold-manifest.json", "Gold manifest")
    oracle_path = _inside(
        run_dir,
        oracle_dir / "hadoop-oracle-manifest.json",
        "Hadoop oracle manifest",
    )
    layer_path = _inside(
        run_dir,
        layer_audit_dir / "layer-audit-manifest.json",
        "layer audit manifest",
    )
    plan = _json_object(plan_path, "run plan")
    gold = _json_object(gold_path, "Gold manifest")
    oracle = _json_object(oracle_path, "Hadoop oracle manifest")
    layers = _json_object(layer_path, "layer audit manifest")
    reconciliation = _json_object(reconciliation_path, "reconciliation report")
    decision = _json_object(decision_report_path, "decision-impact report")
    machine = _json_object(machine_metadata_path, "machine metadata")

    if plan.get("status") != "prepared" or not isinstance(plan.get("run_id"), str):
        raise ValueError("run plan is not prepared")
    run_id = plan["run_id"]
    replay_hash = plan.get("replay_manifest_hash")
    if (
        gold.get("schema_version") != "distributed-gold-manifest-v1"
        or gold.get("status") != "gold_finalized"
        or gold.get("replay_run_id") != run_id
        or gold.get("replay_manifest_hash") != replay_hash
    ):
        raise ValueError("Gold manifest is not bound to the prepared run")
    if (
        oracle.get("schema_version") != "hadoop-oracle-manifest-v2"
        or oracle.get("status") != "complete"
        or oracle.get("run_id") != run_id
    ):
        raise ValueError("Hadoop oracle is not bound to the prepared run")
    if (
        layers.get("schema_version") != "layer-audit-manifest-v1"
        or layers.get("status") != "complete"
        or layers.get("replay_manifest_hash") != replay_hash
        or not isinstance(layers.get("counts"), dict)
    ):
        raise ValueError("layer audit is not bound to the prepared run")
    if (
        reconciliation.get("schema_version") != "reconciliation-v2"
        or reconciliation.get("status") != "passed"
        or reconciliation.get("run_id") != run_id
        or reconciliation.get("replay_manifest_hash") != replay_hash
    ):
        raise ValueError("finalization requires a passed reconciliation-v2 report")
    bindings = reconciliation.get("bindings")
    if not isinstance(bindings, dict):
        raise ValueError("reconciliation bindings are invalid")
    _require_hash(bindings.get("gold_manifest_sha256"), gold_path, "Gold manifest")
    _require_hash(
        bindings.get("hadoop_manifest_sha256"), oracle_path, "Hadoop manifest"
    )
    _require_hash(
        bindings.get("layer_audit_manifest_sha256"),
        layer_path,
        "layer audit manifest",
    )
    if (
        decision.get("schema_version") != "decision-impact-v1"
        or decision.get("status") != "verified"
        or decision.get("run_id") != run_id
        or decision.get("reconciliation_sha256") != _sha256(reconciliation_path)
    ):
        raise ValueError("decision-impact report is not bound to reconciliation")
    scenarios = decision.get("tariff_scenarios")
    if (
        not isinstance(scenarios, list)
        or [
            item.get("normalized_rate") if isinstance(item, dict) else None
            for item in scenarios
        ]
        != ["0.5", "1.0", "2.0"]
        or any(
            item.get("monetary_interpretation")
            != "scenario only; not an HKUST financial result"
            for item in scenarios
            if isinstance(item, dict)
        )
    ):
        raise ValueError("decision-impact tariff scenarios are invalid")
    validate_machine_metadata(machine)

    query_benchmark: dict[str, Any] | None = None
    storage_benchmark: dict[str, Any] | None = None
    if machine["execution_context"] == "designated_machine":
        query_path = run_dir / "reports/query-benchmark.json"
        storage_path = run_dir / "storage/storage-benchmark.json"
        try:
            _inside(run_dir, query_path, "query benchmark")
            _inside(run_dir, storage_path, "storage benchmark")
        except ValueError as exc:
            raise ValueError(
                "designated-machine finalization requires query and storage benchmarks"
            ) from exc
        query_benchmark = _json_object(query_path, "query benchmark")
        storage_benchmark = _json_object(storage_path, "storage benchmark")
        _validate_query_benchmark(
            query_benchmark,
            run_id=run_id,
            gold_manifest_path=gold_path,
            decision_report_path=decision_report_path,
        )
        _validate_storage_benchmark(storage_benchmark, run_id=run_id, plan=plan)

    manifest: dict[str, Any] = {
        "schema_version": "finalized-run-v1",
        "status": "finalized",
        "reconciled": True,
        "immutable": True,
        "replay_run_id": run_id,
        "replay_manifest_hash": replay_hash,
        "reconciliation_sha256": _sha256(reconciliation_path),
        "decision_impact_sha256": _sha256(decision_report_path),
        "machine_metadata_sha256": _sha256(machine_metadata_path),
        "artifacts": _artifact_registry(run_dir),
        "views": _views(
            plan,
            gold,
            layers,
            reconciliation,
            decision,
            machine,
            query_benchmark,
            storage_benchmark,
        ),
    }
    _write_seal(run_dir, manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--oracle-dir", type=Path, required=True)
    parser.add_argument("--layer-audit-dir", type=Path, required=True)
    parser.add_argument("--reconciliation", type=Path, required=True)
    parser.add_argument("--decision-report", type=Path, required=True)
    parser.add_argument("--machine-metadata", type=Path, required=True)
    args = parser.parse_args()
    manifest = seal_distributed_run(
        run_dir=args.run_dir,
        oracle_dir=args.oracle_dir,
        layer_audit_dir=args.layer_audit_dir,
        reconciliation_path=args.reconciliation,
        decision_report_path=args.decision_report,
        machine_metadata_path=args.machine_metadata,
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

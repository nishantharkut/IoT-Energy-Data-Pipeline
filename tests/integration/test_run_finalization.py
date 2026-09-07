from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dashboard.data import REQUIRED_VIEWS, load_finalized_run
from iot_energy_pipeline.run_finalization import seal_distributed_run


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _prepared(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    run = tmp_path / "run"
    oracle = run / "oracle"
    audit = run / "layer-audit"
    reports = run / "reports"
    machine = run / "machine-metadata.json"
    replay_hash = "a" * 64
    _write_json(
        run / "distributed-plan.json",
        {
            "schema_version": "distributed-fixture-plan-v1",
            "status": "prepared",
            "run_id": "sealed-fixture",
            "fault": "combined",
            "seed": 20260825,
            "watermark": "1 minute",
            "canonical_event_count": 3,
            "expected_delivery_count": 5,
            "expected_terminal_count": 3,
            "replay_manifest_hash": replay_hash,
            "canonical_ndjson_sha256": "b" * 64,
        },
    )
    _write_json(
        run / "gold/gold-manifest.json",
        {
            "schema_version": "distributed-gold-manifest-v1",
            "status": "gold_finalized",
            "replay_run_id": "sealed-fixture",
            "replay_manifest_hash": replay_hash,
            "source_coverage_count": 3,
        },
    )
    _write_json(
        oracle / "hadoop-oracle-manifest.json",
        {
            "schema_version": "hadoop-oracle-manifest-v2",
            "status": "complete",
            "run_id": "sealed-fixture",
        },
    )
    _write_json(
        audit / "layer-audit-manifest.json",
        {
            "schema_version": "layer-audit-manifest-v1",
            "status": "complete",
            "replay_manifest_hash": replay_hash,
            "counts": {
                "bronze_deliveries": 5,
                "parsed_valid_deliveries": 4,
                "quarantine_deliveries": 1,
                "silver_deliveries": 3,
                "duplicate_valid_deliveries": 1,
                "late_valid_deliveries": 0,
                "terminal_records": 3,
            },
            "disposition_counts": {
                "silver": 3,
                "duplicate_valid": 1,
                "quarantine": 1,
            },
        },
    )
    reconciliation = reports / "reconciliation.json"
    _write_json(
        reconciliation,
        {
            "schema_version": "reconciliation-v2",
            "status": "passed",
            "run_id": "sealed-fixture",
            "replay_manifest_hash": replay_hash,
            "layer_counts": {
                "bronze_deliveries": 5,
                "silver_deliveries": 3,
                "quarantine_deliveries": 1,
            },
            "aggregate_comparison": {
                "spark_record_count": 1,
                "hadoop_record_count": 1,
                "errors": [],
            },
            "source_record_comparison": {
                "spark_record_count": 3,
                "hadoop_record_count": 3,
                "errors": [],
            },
            "bindings": {
                "gold_manifest_sha256": _hash(run / "gold/gold-manifest.json"),
                "hadoop_manifest_sha256": _hash(oracle / "hadoop-oracle-manifest.json"),
                "layer_audit_manifest_sha256": _hash(
                    audit / "layer-audit-manifest.json"
                ),
            },
        },
    )
    decision = reports / "decision-impact.json"
    _write_json(
        decision,
        {
            "schema_version": "decision-impact-v1",
            "status": "verified",
            "run_id": "sealed-fixture",
            "reconciliation_sha256": _hash(reconciliation),
            "affected_meter_period_count": 1,
            "absolute_kwh_discrepancy": "2",
            "candidate_flagged_record_count": 1,
            "verified_flagged_record_count": 1,
            "verification_seconds": 7.5,
            "candidate_storage_bytes": 100,
            "verification_storage_bytes": 400,
            "storage_overhead_ratio": 4.0,
            "tariff_scenarios": [
                {
                    "normalized_rate": rate,
                    "gross_exposure": exposure,
                    "monetary_interpretation": (
                        "scenario only; not an HKUST financial result"
                    ),
                }
                for rate, exposure in (("0.5", "1"), ("1.0", "2"), ("2.0", "4"))
            ],
        },
    )
    _write_json(
        machine,
        {
            "schema_version": "machine-metadata-v1",
            "captured_at_utc": "2026-08-26T10:00:00Z",
            "execution_context": "fixture",
            "hostname": "coursework-host",
            "operating_system": "Windows",
            "processor": "fixture-cpu",
            "logical_cpu_count": 8,
            "memory_bytes": 17179869184,
            "software": {
                "python": "3.11.9",
                "docker": "deferred",
                "kafka": "apache/kafka:3.7.1",
                "spark": "3.5.1",
                "hadoop": "3.3.6",
            },
        },
    )
    return run, oracle, audit, reconciliation, decision


def test_finalizer_seals_self_contained_verified_run(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)

    manifest = seal_distributed_run(
        run_dir=run,
        oracle_dir=oracle,
        layer_audit_dir=audit,
        reconciliation_path=reconciliation,
        decision_report_path=decision,
        machine_metadata_path=run / "machine-metadata.json",
    )
    loaded = load_finalized_run(run)

    assert manifest == loaded
    assert manifest["replay_run_id"] == "sealed-fixture"
    assert set(manifest["views"]) == set(REQUIRED_VIEWS)
    assert "reports__reconciliation_json" in manifest["artifacts"]
    assert (
        manifest["views"]["energy_quality"]["rows"][0]["monetary_interpretation"]
        == "hypothetical scenarios only; not HKUST financial results"
    )


def test_finalizer_rejects_failed_or_replaced_reconciliation(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    value = json.loads(reconciliation.read_text("utf-8"))
    value["status"] = "failed"
    _write_json(reconciliation, value)

    with pytest.raises(ValueError, match="passed reconciliation"):
        seal_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            decision_report_path=decision,
            machine_metadata_path=run / "machine-metadata.json",
        )


def test_finalizer_rejects_evidence_outside_run_directory(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    external = tmp_path / "external-oracle"
    oracle.rename(external)

    with pytest.raises(ValueError, match="inside the run directory"):
        seal_distributed_run(
            run_dir=run,
            oracle_dir=external,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            decision_report_path=decision,
            machine_metadata_path=run / "machine-metadata.json",
        )


def test_finalizer_is_immutable(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    (run / "run-manifest.json").write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="already sealed"):
        seal_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            decision_report_path=decision,
            machine_metadata_path=run / "machine-metadata.json",
        )


def test_designated_machine_run_requires_query_and_storage_benchmarks(
    tmp_path: Path,
) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    machine_path = run / "machine-metadata.json"
    machine = json.loads(machine_path.read_text("utf-8"))
    machine["execution_context"] = "designated_machine"
    _write_json(machine_path, machine)

    with pytest.raises(ValueError, match="benchmark"):
        seal_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            decision_report_path=decision,
            machine_metadata_path=machine_path,
        )


def test_designated_machine_benchmarks_are_bound_into_dashboard_view(
    tmp_path: Path,
) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    machine_path = run / "machine-metadata.json"
    machine = json.loads(machine_path.read_text("utf-8"))
    machine["execution_context"] = "designated_machine"
    _write_json(machine_path, machine)
    _write_json(
        run / "reports/query-benchmark.json",
        {
            "schema_version": "query-benchmark-v1",
            "status": "complete",
            "run_id": "sealed-fixture",
            "gold_manifest_sha256": _hash(run / "gold/gold-manifest.json"),
            "decision_impact_sha256": _hash(decision),
            "parameters": {
                "meter_id": "meter-a",
                "building_id": "building-a",
                "window_start_utc": "2026-01-01T00:00:00Z",
                "window_end_utc": "2026-01-02T00:00:00Z",
            },
            "results": [
                {
                    "query": name,
                    "repetitions": 3,
                    "row_count": 1,
                    "seconds": {"median": 0.1, "minimum": 0.05, "maximum": 0.2},
                    "sql_sha256": "c" * 64,
                }
                for name in (
                    "meter",
                    "building",
                    "time_window",
                    "quality",
                    "exposure",
                )
            ],
        },
    )
    _write_json(
        run / "storage/storage-benchmark.json",
        {
            "schema_version": "storage-benchmark-v1",
            "status": "complete",
            "run_id": "sealed-fixture",
            "record_count": 3,
            "formats": {
                "ndjson": {
                    "byte_size": 100,
                    "file_count": 1,
                    "sha256": "b" * 64,
                    "write_seconds": None,
                    "relative_to_ndjson": 1.0,
                },
                "parquet_zstd_unpartitioned": {
                    "byte_size": 50,
                    "file_count": 2,
                    "write_seconds": 1.0,
                    "relative_to_ndjson": 0.5,
                },
                "parquet_zstd_partitioned": {
                    "byte_size": 75,
                    "file_count": 3,
                    "partition_columns": ["source_variant", "meter_id"],
                    "write_seconds": 1.5,
                    "relative_to_ndjson": 0.75,
                },
            },
        },
    )

    manifest = seal_distributed_run(
        run_dir=run,
        oracle_dir=oracle,
        layer_audit_dir=audit,
        reconciliation_path=reconciliation,
        decision_report_path=decision,
        machine_metadata_path=machine_path,
    )

    row = manifest["views"]["storage_queries"]["rows"][0]
    assert row["query_results"][0]["query"] == "meter"
    assert row["storage_formats"]["ndjson"]["byte_size"] == 100


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda query, storage: query["results"][0].update(repetitions=2), "query"),
        (
            lambda query, storage: query["results"][0]["seconds"].pop("maximum"),
            "query",
        ),
        (
            lambda query, storage: storage["formats"][
                "parquet_zstd_unpartitioned"
            ].update(relative_to_ndjson=0.75),
            "storage",
        ),
        (lambda query, storage: storage.pop("record_count"), "storage"),
    ],
)
def test_designated_machine_rejects_incomplete_or_inconsistent_benchmarks(
    tmp_path: Path, mutation: object, message: str
) -> None:
    run, oracle, audit, reconciliation, decision = _prepared(tmp_path)
    machine_path = run / "machine-metadata.json"
    machine = json.loads(machine_path.read_text("utf-8"))
    machine["execution_context"] = "designated_machine"
    _write_json(machine_path, machine)
    query = {
        "schema_version": "query-benchmark-v1",
        "status": "complete",
        "run_id": "sealed-fixture",
        "gold_manifest_sha256": _hash(run / "gold/gold-manifest.json"),
        "decision_impact_sha256": _hash(decision),
        "parameters": {
            "meter_id": "meter-a",
            "building_id": "building-a",
            "window_start_utc": "2026-01-01T00:00:00Z",
            "window_end_utc": "2026-01-02T00:00:00Z",
        },
        "results": [
            {
                "query": name,
                "repetitions": 3,
                "row_count": 1,
                "seconds": {"median": 0.1, "minimum": 0.05, "maximum": 0.2},
                "sql_sha256": "c" * 64,
            }
            for name in ("meter", "building", "time_window", "quality", "exposure")
        ],
    }
    storage = {
        "schema_version": "storage-benchmark-v1",
        "status": "complete",
        "run_id": "sealed-fixture",
        "record_count": 3,
        "formats": {
            "ndjson": {
                "byte_size": 100,
                "file_count": 1,
                "sha256": "b" * 64,
                "write_seconds": None,
                "relative_to_ndjson": 1.0,
            },
            "parquet_zstd_unpartitioned": {
                "byte_size": 50,
                "file_count": 2,
                "write_seconds": 1.0,
                "relative_to_ndjson": 0.5,
            },
            "parquet_zstd_partitioned": {
                "byte_size": 75,
                "file_count": 3,
                "partition_columns": ["source_variant", "meter_id"],
                "write_seconds": 1.5,
                "relative_to_ndjson": 0.75,
            },
        },
    }
    assert callable(mutation)
    mutation(query, storage)  # type: ignore[operator]
    _write_json(run / "reports/query-benchmark.json", query)
    _write_json(run / "storage/storage-benchmark.json", storage)

    with pytest.raises(ValueError, match=message):
        seal_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            decision_report_path=decision,
            machine_metadata_path=machine_path,
        )

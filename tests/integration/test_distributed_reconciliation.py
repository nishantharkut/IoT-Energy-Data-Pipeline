from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from iot_energy_pipeline.run_reconciliation import reconcile_distributed_run


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def _ndjson(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in records
        ),
        encoding="utf-8",
    )


def _descriptor(path: Path, count: int, name: str) -> dict:
    return {
        "file": name,
        "local_file": name,
        "record_count": count,
        "byte_size": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _prepared(tmp_path: Path) -> tuple[Path, Path, Path, str]:
    run = tmp_path / "run"
    oracle = tmp_path / "oracle"
    audit = tmp_path / "audit"
    replay_hash = "a" * 64
    source = {"source_event_id": "1" * 64, "meter_id": "meter-a", "value": 10}
    aggregate = {"group_key": "meter-a|kWh|2", "count": 1, "sum_scaled": 10}
    canonical = run / "snapshot/canonical.ndjson"
    _ndjson(canonical, [source])
    _json(
        run / "distributed-plan.json",
        {
            "status": "prepared",
            "run_id": "fixture-run",
            "replay_manifest_hash": replay_hash,
            "canonical_ndjson_path": "snapshot/canonical.ndjson",
            "canonical_event_count": 1,
            "expected_delivery_count": 2,
            "expected_terminal_count": 1,
        },
    )
    gold_source = run / "gold/gold-source-events.ndjson"
    gold_aggregate = run / "gold/gold-aggregates.ndjson"
    _ndjson(gold_source, [source])
    _ndjson(gold_aggregate, [aggregate])
    _json(
        run / "gold/gold-manifest.json",
        {
            "schema_version": "distributed-gold-manifest-v1",
            "status": "gold_finalized",
            "replay_run_id": "fixture-run",
            "replay_manifest_hash": replay_hash,
            "bronze_delivery_count": 2,
            "terminal_record_count": 1,
            "source_coverage_count": 1,
            "aggregate_count": 1,
            "artifacts": {
                "gold_source_events_ndjson": _descriptor(
                    gold_source, 1, "gold-source-events.ndjson"
                ),
                "gold_aggregates_ndjson": _descriptor(
                    gold_aggregate, 1, "gold-aggregates.ndjson"
                ),
            },
        },
    )
    oracle_source = oracle / "hadoop-source-records.ndjson"
    oracle_aggregate = oracle / "hadoop-aggregates.ndjson"
    _ndjson(oracle_source, [source])
    _ndjson(oracle_aggregate, [aggregate])
    _json(
        oracle / "hadoop-oracle-manifest.json",
        {
            "schema_version": "hadoop-oracle-manifest-v2",
            "status": "complete",
            "run_id": "fixture-run",
            "input": {
                "local_sha256": hashlib.sha256(canonical.read_bytes()).hexdigest()
            },
            "outputs": {
                "aggregates": _descriptor(
                    oracle_aggregate, 1, "hadoop-aggregates.ndjson"
                ),
                "source_records": _descriptor(
                    oracle_source, 1, "hadoop-source-records.ndjson"
                ),
            },
        },
    )
    disposition = audit / "layer-dispositions.ndjson"
    _ndjson(disposition, [{"delivery_id": "one"}, {"delivery_id": "two"}])
    candidate = audit / "silver-candidate-events.ndjson"
    _ndjson(candidate, [source])
    _json(
        audit / "layer-audit-manifest.json",
        {
            "schema_version": "layer-audit-manifest-v1",
            "status": "complete",
            "replay_manifest_hash": replay_hash,
            "counts": {
                "bronze_deliveries": 2,
                "parsed_valid_deliveries": 2,
                "quarantine_deliveries": 0,
                "silver_deliveries": 1,
                "duplicate_valid_deliveries": 1,
                "late_valid_deliveries": 0,
                "terminal_records": 1,
            },
            "artifacts": {
                "layer_dispositions_ndjson": _descriptor(
                    disposition, 2, "layer-dispositions.ndjson"
                ),
                "silver_candidate_events_ndjson": _descriptor(
                    candidate, 1, "silver-candidate-events.ndjson"
                ),
            },
        },
    )
    return run, oracle, audit, replay_hash


def test_reconciliation_binds_layers_and_compares_both_oracle_outputs(
    tmp_path: Path,
) -> None:
    run, oracle, audit, replay_hash = _prepared(tmp_path)

    report = reconcile_distributed_run(
        run_dir=run,
        oracle_dir=oracle,
        layer_audit_dir=audit,
        output=run / "reports/reconciliation.json",
    )

    assert report["status"] == "passed"
    assert report["replay_manifest_hash"] == replay_hash
    assert report["aggregate_comparison"]["errors"] == []
    assert report["source_record_comparison"]["errors"] == []
    assert report["source_record_comparison"]["exact_fields"] == [
        "meter_id",
        "source_event_id",
        "value",
    ]
    assert report["bindings"]["silver_candidate_events_sha256"]


def test_field_difference_fails_reconciliation_without_finalizing(
    tmp_path: Path,
) -> None:
    run, oracle, audit, _ = _prepared(tmp_path)
    oracle_source = oracle / "hadoop-source-records.ndjson"
    changed = {"source_event_id": "1" * 64, "meter_id": "meter-a", "value": 11}
    _ndjson(oracle_source, [changed])
    manifest_path = oracle / "hadoop-oracle-manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["outputs"]["source_records"] = _descriptor(
        oracle_source, 1, "hadoop-source-records.ndjson"
    )
    _json(manifest_path, manifest)

    report = reconcile_distributed_run(
        run_dir=run,
        oracle_dir=oracle,
        layer_audit_dir=audit,
        output=run / "reports/reconciliation.json",
    )

    assert report["status"] == "failed"
    assert "value" in report["source_record_comparison"]["errors"][0]


def test_accounting_or_artifact_tampering_is_rejected(tmp_path: Path) -> None:
    run, oracle, audit, _ = _prepared(tmp_path)
    (run / "gold/gold-aggregates.ndjson").write_text("tampered\n", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact integrity"):
        reconcile_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            output=run / "reports/reconciliation.json",
        )


def test_reconciliation_output_is_immutable(tmp_path: Path) -> None:
    run, oracle, audit, _ = _prepared(tmp_path)
    output = run / "reports/reconciliation.json"
    output.parent.mkdir(parents=True)
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="immutable reconciliation"):
        reconcile_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            output=output,
        )


def test_reconciliation_rejects_non_integer_layer_counts(tmp_path: Path) -> None:
    run, oracle, audit, _ = _prepared(tmp_path)
    manifest_path = audit / "layer-audit-manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["counts"]["parsed_valid_deliveries"] = "2"
    _json(manifest_path, manifest)

    with pytest.raises(ValueError, match="layer count.*invalid"):
        reconcile_distributed_run(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            output=run / "reports/reconciliation.json",
        )

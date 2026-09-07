from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from iot_energy_pipeline.business_report import build_distributed_business_report


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", "utf-8")


def _write_ndjson(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in records), "utf-8"
    )


def _descriptor(path: Path, count: int, field: str) -> dict:
    return {
        field: path.name,
        "record_count": count,
        "byte_size": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def _reading(index: int, value: int) -> dict:
    return {
        "source_event_id": f"{index:064x}",
        "meter_id": "meter-a",
        "event_time_utc": f"2024-01-01T0{index}:00:00Z",
        "scaled_value": value,
        "decimal_scale": 2,
        "measurement_kind": "cumulative_energy",
        "unit": "kWh",
    }


def _prepared(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    run = tmp_path / "run"
    oracle = tmp_path / "oracle"
    audit = tmp_path / "audit"
    reconciliation = run / "reports/reconciliation.json"
    verified = [_reading(0, 1000), _reading(1, 1100), _reading(2, 1200)]
    candidate = [verified[0], verified[2]]
    gold_source = run / "gold/gold-source-events.ndjson"
    gold_aggregates = run / "gold/gold-aggregates.ndjson"
    _write_ndjson(gold_source, verified)
    _write_ndjson(gold_aggregates, [{"group_key": "meter-a|kWh|2"}])
    _write_json(
        run / "gold/gold-manifest.json",
        {
            "schema_version": "distributed-gold-manifest-v1",
            "status": "gold_finalized",
            "replay_run_id": "business-fixture",
            "replay_manifest_hash": "a" * 64,
            "gold_finalization_seconds": 2.5,
            "artifacts": {
                "gold_source_events_ndjson": _descriptor(gold_source, 3, "file"),
                "gold_aggregates_ndjson": _descriptor(gold_aggregates, 1, "file"),
            },
        },
    )
    candidate_path = audit / "silver-candidate-events.ndjson"
    disposition = audit / "layer-dispositions.ndjson"
    _write_ndjson(candidate_path, candidate)
    _write_ndjson(disposition, [{"delivery_id": "one"}])
    _write_json(
        audit / "layer-audit-manifest.json",
        {
            "schema_version": "layer-audit-manifest-v1",
            "status": "complete",
            "replay_manifest_hash": "a" * 64,
            "layer_audit_seconds": 0.75,
            "artifacts": {
                "silver_candidate_events_ndjson": _descriptor(
                    candidate_path, 2, "file"
                ),
                "layer_dispositions_ndjson": _descriptor(disposition, 1, "file"),
            },
        },
    )
    oracle_source = oracle / "hadoop-source-records.ndjson"
    oracle_aggregate = oracle / "hadoop-aggregates.ndjson"
    _write_ndjson(oracle_source, verified)
    _write_ndjson(oracle_aggregate, [{"group_key": "meter-a|kWh|2"}])
    _write_json(
        oracle / "hadoop-oracle-manifest.json",
        {
            "schema_version": "hadoop-oracle-manifest-v2",
            "status": "complete",
            "run_id": "business-fixture",
            "verification_seconds": 4.0,
            "outputs": {
                "source_records": _descriptor(oracle_source, 3, "local_file"),
                "aggregates": _descriptor(oracle_aggregate, 1, "local_file"),
            },
        },
    )
    _write_json(
        reconciliation,
        {
            "schema_version": "reconciliation-v2",
            "status": "passed",
            "run_id": "business-fixture",
            "replay_manifest_hash": "a" * 64,
            "reconciliation_seconds": 0.25,
            "bindings": {
                "gold_manifest_sha256": hashlib.sha256(
                    (run / "gold/gold-manifest.json").read_bytes()
                ).hexdigest(),
                "hadoop_manifest_sha256": hashlib.sha256(
                    (oracle / "hadoop-oracle-manifest.json").read_bytes()
                ).hexdigest(),
                "layer_audit_manifest_sha256": hashlib.sha256(
                    (audit / "layer-audit-manifest.json").read_bytes()
                ).hexdigest(),
                "silver_candidate_events_sha256": hashlib.sha256(
                    candidate_path.read_bytes()
                ).hexdigest(),
            },
        },
    )
    return run, oracle, audit, reconciliation


def test_distributed_business_report_is_bound_and_explicitly_hypothetical(
    tmp_path: Path,
) -> None:
    run, oracle, audit, reconciliation = _prepared(tmp_path)

    report = build_distributed_business_report(
        run_dir=run,
        oracle_dir=oracle,
        layer_audit_dir=audit,
        reconciliation_path=reconciliation,
        output=run / "reports/decision-impact.json",
    )

    assert report["status"] == "verified"
    assert report["run_id"] == "business-fixture"
    assert report["verification_seconds"] == 7.5
    assert report["candidate_flagged_records"]
    assert report["verified_flagged_records"]
    assert report["verification_storage_scope"]
    assert all(
        scenario["monetary_interpretation"]
        == "scenario only; not an HKUST financial result"
        for scenario in report["tariff_scenarios"]
    )


def test_business_report_refuses_failed_reconciliation(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation = _prepared(tmp_path)
    value = json.loads(reconciliation.read_text("utf-8"))
    value["status"] = "failed"
    _write_json(reconciliation, value)

    with pytest.raises(ValueError, match="passed reconciliation"):
        build_distributed_business_report(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            output=run / "reports/decision-impact.json",
        )


def test_business_report_rejects_artifacts_changed_after_reconciliation(
    tmp_path: Path,
) -> None:
    run, oracle, audit, reconciliation = _prepared(tmp_path)
    source = run / "gold/gold-source-events.ndjson"
    records = [json.loads(line) for line in source.read_text("utf-8").splitlines()]
    records[-1]["scaled_value"] = 9999
    _write_ndjson(source, records)
    manifest_path = run / "gold/gold-manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["artifacts"]["gold_source_events_ndjson"] = _descriptor(source, 3, "file")
    _write_json(manifest_path, manifest)

    with pytest.raises(ValueError, match="Gold manifest binding"):
        build_distributed_business_report(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            output=run / "reports/decision-impact.json",
        )


def test_business_report_output_is_immutable(tmp_path: Path) -> None:
    run, oracle, audit, reconciliation = _prepared(tmp_path)
    output = run / "reports/decision-impact.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="immutable decision-impact"):
        build_distributed_business_report(
            run_dir=run,
            oracle_dir=oracle,
            layer_audit_dir=audit,
            reconciliation_path=reconciliation,
            output=output,
        )

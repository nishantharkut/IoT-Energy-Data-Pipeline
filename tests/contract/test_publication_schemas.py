from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "filename,required",
    [
        (
            "replay-schedule-v2.schema.json",
            {
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
            },
        ),
        (
            "replay-receipt-v2.schema.json",
            {
                "schema_version",
                "status",
                "replay_run_id",
                "replay_manifest_hash",
                "schedule_hash",
                "expected_delivery_count",
                "acknowledged_delivery_count",
                "acknowledgement_ledger",
                "terminal_offsets",
            },
        ),
        (
            "reconciliation-v2.schema.json",
            {
                "schema_version",
                "status",
                "run_id",
                "replay_manifest_hash",
                "layer_counts",
                "aggregate_comparison",
                "source_record_comparison",
                "bindings",
                "reconciliation_seconds",
            },
        ),
        (
            "decision-impact-v1.schema.json",
            {
                "schema_version",
                "status",
                "run_id",
                "reconciliation_sha256",
                "affected_meter_period_count",
                "affected_meter_periods",
                "absolute_kwh_discrepancy",
                "candidate_flagged_record_count",
                "candidate_flagged_records",
                "verified_flagged_record_count",
                "verified_flagged_records",
                "verification_seconds",
                "verification_time_components",
                "candidate_storage_bytes",
                "verification_storage_bytes",
                "verification_storage_scope",
                "storage_overhead_bytes",
                "storage_overhead_ratio",
                "tariff_scenarios",
            },
        ),
        (
            "machine-metadata-v1.schema.json",
            {
                "schema_version",
                "captured_at_utc",
                "execution_context",
                "hostname",
                "operating_system",
                "processor",
                "logical_cpu_count",
                "memory_bytes",
                "software",
            },
        ),
        (
            "finalized-run-v1.schema.json",
            {
                "schema_version",
                "status",
                "reconciled",
                "immutable",
                "replay_run_id",
                "replay_manifest_hash",
                "reconciliation_sha256",
                "decision_impact_sha256",
                "machine_metadata_sha256",
                "artifacts",
                "views",
            },
        ),
        (
            "experiment-plan-v1.schema.json",
            {
                "schema_version",
                "status",
                "execution_gate",
                "matrix_sha256",
                "snapshot_manifest_sha256",
                "canonical_ndjson_sha256",
                "machine_metadata_sha256",
                "full_event_count",
                "summary_statistics",
                "storage_formats",
                "query_names",
                "normalized_tariff_rates",
                "monetary_interpretation",
                "runs",
            },
        ),
        (
            "experiment-results-v1.schema.json",
            {
                "schema_version",
                "status",
                "experiment_plan_sha256",
                "machine_metadata_sha256",
                "snapshot_manifest_sha256",
                "runs",
                "performance_summary",
                "correctness_semantic_equivalence",
                "recovery_semantic_equivalence",
                "watermark_effect",
                "monetary_interpretation",
            },
        ),
        (
            "query-benchmark-v1.schema.json",
            {
                "schema_version",
                "status",
                "run_id",
                "gold_manifest_sha256",
                "decision_impact_sha256",
                "parameters",
                "results",
            },
        ),
        (
            "storage-benchmark-v1.schema.json",
            {"schema_version", "status", "run_id", "record_count", "formats"},
        ),
    ],
)
def test_publication_schema_is_strict_and_complete(
    filename: str, required: set[str]
) -> None:
    path = Path("schemas") / filename
    schema = json.loads(path.read_text(encoding="utf-8"))

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"].endswith(filename)
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == required
    assert set(schema["properties"]) == required


def test_canonicalization_rules_schema_is_closed_and_evidence_bearing() -> None:
    path = Path("schemas/canonicalization-rules-v1.schema.json")
    schema = json.loads(path.read_text(encoding="utf-8"))

    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {
        "schema_version",
        "source_timezone",
        "timezone_evidence",
        "source_layout_file",
        "source_layout_sha256",
        "measurements",
    }
    measurement = schema["properties"]["measurements"]["additionalProperties"]
    assert measurement["additionalProperties"] is False
    assert "evidence" in measurement["required"]


def test_query_benchmark_schema_closes_every_result_and_summary() -> None:
    schema = json.loads(
        Path("schemas/query-benchmark-v1.schema.json").read_text(encoding="utf-8")
    )

    results = schema["properties"]["results"]
    assert results["minItems"] == results["maxItems"] == 5
    assert results["items"] is False
    assert [
        item["allOf"][1]["properties"]["query"]["const"]
        for item in results["prefixItems"]
    ] == ["meter", "building", "time_window", "quality", "exposure"]
    result = schema["$defs"]["query_result"]
    assert result["additionalProperties"] is False
    assert set(result["required"]) == {
        "query",
        "repetitions",
        "row_count",
        "seconds",
        "sql_sha256",
    }
    assert result["properties"]["repetitions"]["const"] == 3
    summary = schema["$defs"]["summary"]
    assert summary["additionalProperties"] is False
    assert set(summary["required"]) == {"median", "minimum", "maximum"}


def test_storage_benchmark_schema_closes_each_format_descriptor() -> None:
    schema = json.loads(
        Path("schemas/storage-benchmark-v1.schema.json").read_text(encoding="utf-8")
    )

    formats = schema["properties"]["formats"]
    assert formats["additionalProperties"] is False
    for name in (
        "ndjson",
        "parquet_zstd_unpartitioned",
        "parquet_zstd_partitioned",
    ):
        descriptor = schema["$defs"][name]
        assert descriptor["additionalProperties"] is False
        assert formats["properties"][name] == {"$ref": f"#/$defs/{name}"}
    assert schema["$defs"]["ndjson"]["properties"]["file_count"]["const"] == 1
    assert schema["$defs"]["parquet_zstd_partitioned"]["properties"][
        "partition_columns"
    ]["const"] == ["source_variant", "meter_id"]


def test_experiment_results_schema_closes_all_publication_evidence() -> None:
    schema = json.loads(
        Path("schemas/experiment-results-v1.schema.json").read_text(encoding="utf-8")
    )

    for definition in (
        "run",
        "performance_summary",
        "summary",
        "semantic_hashes",
        "correctness_equivalence",
        "recovery_equivalence",
        "watermark_effect",
    ):
        assert schema["$defs"][definition]["additionalProperties"] is False
    assert schema["properties"]["runs"]["items"] == {"$ref": "#/$defs/run"}
    assert schema["properties"]["performance_summary"]["items"] == {
        "$ref": "#/$defs/performance_summary"
    }

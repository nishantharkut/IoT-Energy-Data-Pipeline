from __future__ import annotations

import pytest

from iot_energy_pipeline.course_report import render_course_report


def test_course_report_renders_measured_results_with_scenario_guardrail() -> None:
    results = {
        "status": "complete",
        "experiment_plan_sha256": "a" * 64,
        "machine_metadata_sha256": "b" * 64,
        "snapshot_manifest_sha256": "c" * 64,
        "runs": [
            {
                "run_id": "correctness-combined-r01",
                "experiment": "correctness",
                "event_count": "fixture",
                "status": "finalized",
                "workflow_seconds": 8.0,
            }
        ],
        "performance_summary": [
            {
                "event_count": 100000,
                "repetitions": 3,
                "metrics": {
                    "workflow_seconds": {
                        "median": 10.0,
                        "minimum": 9.0,
                        "maximum": 11.0,
                    }
                },
            }
        ],
        "correctness_semantic_equivalence": {"status": "identical"},
        "recovery_semantic_equivalence": {"status": "identical"},
        "watermark_effect": {
            "gold_status": "identical",
            "candidate_difference_exposed": True,
        },
    }
    decisions = {
        "correctness-combined-r01": {
            "affected_meter_period_count": 1,
            "absolute_kwh_discrepancy": "2",
            "verification_seconds": 4.5,
            "tariff_scenarios": [{"normalized_rate": "1.0", "gross_exposure": "2"}],
        }
    }

    report = render_course_report(results, decisions)

    assert "100,000" in report
    assert "median" in report.lower()
    assert "Recovery records | identical" in report
    assert "Fault schedules | identical" in report
    assert "Restrictive watermark | candidate difference exposed" in report
    assert "not an HKUST financial result" in report
    assert "correctness-combined-r01" in report


def test_course_report_refuses_incomplete_results() -> None:
    with pytest.raises(ValueError, match="complete experiment results"):
        render_course_report({"status": "running"}, {})

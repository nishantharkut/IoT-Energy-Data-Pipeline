from __future__ import annotations

from datetime import timedelta

import pytest

from iot_energy_pipeline.contracts import SourceLocator, source_event_id, to_scaled_int
from iot_energy_pipeline.reports import decision_impact
from iot_energy_pipeline.schedule import build_schedule, partition_for
from jobs.hadoop.reconcile import reconcile
from jobs.spark.finalize import finalize_gold
from jobs.spark.stream import process_deliveries


def event(n: int) -> dict:
    return {
        "schema_version": "canonical-event-v1",
        "source_event_id": f"{n:064x}",
        "meter_id": "m1",
        "unit": "UNKNOWN",
        "decimal_scale": 2,
        "scaled_value": n * 100,
        "event_time_utc": f"2024-01-01T00:0{n}:00Z",
        "quality_flags": [],
    }


def test_source_identity_is_structural() -> None:
    a = SourceLocator("v1", "clean", "a.xlsx", "Sheet", 2)
    b = SourceLocator("v1", "clean", "a.xlsx", "Sheet", 3)
    assert source_event_id(a) != source_event_id(b)
    assert source_event_id(a) == source_event_id(a)


def test_scale_is_exact_and_bounded() -> None:
    assert to_scaled_int("1.2300", 4) == 12300
    with pytest.raises(ValueError):
        to_scaled_int("1.231", 2)
    with pytest.raises(OverflowError):
        to_scaled_int("9223372036854775808", 0)


def test_faulted_run_preserves_bronze_and_recovers_gold() -> None:
    canonical = [event(1), event(2)]
    schedule = build_schedule(canonical, "fixture", seed=7, fault="combined")
    deliveries = [
        {"delivery_id": d.delivery_id, "payload": d.payload}
        for d in schedule.deliveries
    ]
    layers = process_deliveries(
        deliveries, {e["source_event_id"] for e in canonical}, timedelta(days=1)
    )
    assert layers["counts"]["bronze_deliveries"] == len(deliveries)
    assert len(layers["quarantine"]) == 2
    gold = finalize_gold(
        [
            {"event": item["event"], "delivery_id": item["delivery_id"]}
            for item in layers["parsed"]
        ],
        canonical,
    )
    assert gold[0]["count"] == 2


def test_partition_and_reconciliation_are_deterministic() -> None:
    assert partition_for("a", 4) == partition_for("a", 4)
    expected = [{"group_key": "m", "count": 1, "sum_scaled": 2}]
    assert reconcile(expected, expected) == []
    assert reconcile(expected, [{**expected[0], "sum_scaled": 3}])


def test_tariffs_are_explicit_scenarios() -> None:
    result = decision_impact(10, 8)
    assert [row["rate"] for row in result] == [0.5, 1.0, 2.0]

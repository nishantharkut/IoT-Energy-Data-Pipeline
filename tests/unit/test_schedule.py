from __future__ import annotations

import hashlib
import json

import pytest

from iot_energy_pipeline.schedule import (
    ReplaySchedule,
    build_schedule,
    load_schedule,
    max_event_time_lateness,
    partition_for,
)


def _events() -> list[dict]:
    return [
        {
            "source_event_id": hashlib.sha256(f"row-{index}".encode()).hexdigest(),
            "event_time_utc": f"2024-01-01T0{index}:00:00Z",
            "meter_id": "meter-a",
            "scaled_value": index,
        }
        for index in range(4)
    ]


@pytest.mark.parametrize(
    "fault,expected_multiplier",
    [
        ("clean", 1),
        ("duplicate", 2),
        ("delayed", 1),
        ("malformed", 2),
        ("combined", 3),
    ],
)
def test_fault_schedules_preserve_one_valid_delivery_per_source(
    fault: str, expected_multiplier: int
) -> None:
    events = _events()

    schedule = build_schedule(events, "run-1", seed=73, fault=fault)

    valid = [d for d in schedule.deliveries if d.injection_type == "valid"]
    assert {d.source_event_id for d in valid} == {
        event["source_event_id"] for event in events
    }
    assert len(valid) == len(events)
    assert len(schedule.deliveries) == len(events) * expected_multiplier
    assert len({d.delivery_id for d in schedule.deliveries}) == len(schedule.deliveries)


def test_schedule_is_reproducible_and_input_order_independent() -> None:
    events = _events()

    first = build_schedule(events, "run-1", seed=73, fault="combined")
    second = build_schedule(reversed(events), "run-1", seed=73, fault="combined")

    assert first.to_dict() == second.to_dict()


def test_schedule_artifact_round_trips_with_contract_validation(tmp_path) -> None:
    schedule = build_schedule(_events(), "run-1", seed=73, fault="combined")
    path = tmp_path / "schedule.json"
    path.write_text(json.dumps(schedule.to_dict()), encoding="utf-8")

    loaded = load_schedule(path)

    assert isinstance(loaded, ReplaySchedule)
    assert loaded == schedule


def test_tampered_schedule_artifact_is_rejected(tmp_path) -> None:
    schedule = build_schedule(_events(), "run-1", seed=73, fault="clean")
    raw = schedule.to_dict()
    raw["deliveries"][0]["payload"] = "{}"
    path = tmp_path / "schedule.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="payload digest"):
        load_schedule(path)


def test_delayed_schedule_has_controlled_lateness() -> None:
    events = _events()
    clean = build_schedule(events, "run-1", seed=73, fault="clean")
    delayed = build_schedule(events, "run-1", seed=73, fault="delayed")

    assert max_event_time_lateness(clean) == 0
    assert max_event_time_lateness(delayed) > 0
    assert delayed.delayed_source_event_ids


def test_partition_formula_uses_first_eight_sha256_bytes() -> None:
    source_id = "abc123"
    expected = (
        int.from_bytes(hashlib.sha256(source_id.encode()).digest()[:8], "big") % 7
    )

    assert partition_for(source_id, 7) == expected


def test_delivery_identity_binds_the_resolved_schedule_position() -> None:
    schedule = build_schedule(
        _events(), run_id="position-bound", seed=19, fault="combined"
    )

    for delivery in schedule.deliveries:
        expected = hashlib.sha256(
            (
                f"position-bound|19|{delivery.source_event_id}|"
                f"{delivery.injection_type}|{delivery.position}"
            ).encode("utf-8")
        ).hexdigest()
        assert delivery.delivery_id == expected


@pytest.mark.parametrize("count", [0, -1, True])
def test_invalid_partition_count_fails(count: int) -> None:
    with pytest.raises(ValueError):
        partition_for("event", count)

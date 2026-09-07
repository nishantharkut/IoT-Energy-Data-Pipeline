from __future__ import annotations

import hashlib
import json
from datetime import timedelta

import pytest

from iot_energy_pipeline.schedule import build_schedule
from jobs.spark.common import (
    QUARANTINE_CATEGORIES,
    archive_kafka_record,
    parse_delivery,
)
from jobs.spark.stream import assert_retaining_watermark, process_deliveries

MANIFEST_HASH = "f" * 64


def _event(index: int, *, unit: str = "kWh", scale: int = 2) -> dict:
    return {
        "schema_version": "canonical-event-v1",
        "dataset_version": "fixture-v1",
        "source_variant": "clean",
        "source_event_id": hashlib.sha256(f"row-{index}".encode()).hexdigest(),
        "source_relative_path": "readings.xlsx",
        "workbook_sheet": "Readings",
        "source_row_number": index + 2,
        "meter_id": "meter-a",
        "brick_entity_id": "https://example.test/meter-a",
        "brick_building_ids": ["https://example.test/building-a"],
        "brick_zone_ids": ["https://example.test/zone-a"],
        "brick_metered_entities": [
            {
                "entity_id": "https://example.test/panel-a",
                "type_uris": [
                    "https://brickschema.org/schema/Brick#Electrical_Equipment"
                ],
            }
        ],
        "brick_unit_uri": "http://qudt.org/vocab/unit/KiloW-HR",
        "brick_usage_type": "Normal",
        "original_timestamp_text": f"2024-01-01T0{index}:00:00",
        "source_timezone": "Asia/Hong_Kong",
        "event_time_utc": f"2024-01-01T0{index}:00:00Z",
        "original_numeric_text": f"{index}.00",
        "scaled_value": index * 100,
        "decimal_scale": scale,
        "measurement_kind": "cumulative_energy",
        "unit": unit,
        "quality_flags": [],
    }


def _headers(**changes: str) -> dict[str, str]:
    result = {
        "record_type": "data",
        "replay_manifest_hash": MANIFEST_HASH,
    }
    result.update(changes)
    return result


def test_bronze_record_preserves_complete_kafka_delivery_metadata() -> None:
    record = archive_kafka_record(
        topic="fixture-events",
        key="source-id",
        value="raw-json",
        headers=(("delivery_id", "delivery-1"), ("record_type", "data")),
        partition=2,
        offset=41,
        kafka_timestamp="2024-01-01T00:00:01Z",
        ingestion_timestamp="2024-01-01T00:00:02Z",
    )

    assert record == {
        "topic": "fixture-events",
        "key": "source-id",
        "value": "raw-json",
        "headers": [
            {"key": "delivery_id", "value": "delivery-1"},
            {"key": "record_type", "value": "data"},
        ],
        "partition": 2,
        "offset": 41,
        "kafka_timestamp": "2024-01-01T00:00:01Z",
        "ingestion_timestamp": "2024-01-01T00:00:02Z",
    }


@pytest.mark.parametrize(
    "raw,headers,known,category",
    [
        ("{broken", _headers(), {}, "invalid_json"),
        ("[]", _headers(), {}, "schema_mismatch"),
        (json.dumps({"meter_id": "m"}), _headers(), {}, "missing_required_identity"),
        (
            json.dumps(_event(1)),
            _headers(replay_manifest_hash="e" * 64),
            {_event(1)["source_event_id"]: _event(1)},
            "run_manifest_mismatch",
        ),
        (json.dumps(_event(1)), _headers(), {}, "unknown_source_event_id"),
        (
            json.dumps({**_event(1), "event_time_utc": "not-a-time"}),
            _headers(),
            {_event(1)["source_event_id"]: _event(1)},
            "invalid_timestamp",
        ),
        (
            json.dumps({**_event(1), "scaled_value": "bad"}),
            _headers(),
            {_event(1)["source_event_id"]: _event(1)},
            "invalid_numeric_value",
        ),
        (
            json.dumps({**_event(1), "scaled_value": 2**63}),
            _headers(),
            {_event(1)["source_event_id"]: _event(1)},
            "numeric_overflow",
        ),
        (
            json.dumps({**_event(1), "unit": "Wh"}),
            _headers(),
            {_event(1)["source_event_id"]: _event(1)},
            "unit_or_scale_contract_violation",
        ),
    ],
)
def test_quarantine_taxonomy_is_finite_and_specific(
    raw: str,
    headers: dict[str, str],
    known: dict[str, dict],
    category: str,
) -> None:
    event, actual = parse_delivery(raw, known, MANIFEST_HASH, headers)

    assert event is None
    assert actual == category
    assert actual in QUARANTINE_CATEGORIES


def _bronze(schedule) -> list[dict]:
    return [
        {
            "delivery_id": delivery.delivery_id,
            "partition": delivery.partition,
            "offset": delivery.position,
            "payload": delivery.payload,
            "headers": _headers(),
        }
        for delivery in schedule.deliveries
    ]


def test_late_valid_event_is_candidate_loss_not_quarantine() -> None:
    events = [_event(index) for index in range(4)]
    schedule = build_schedule(events, "delayed", seed=7, fault="delayed")
    known = {event["source_event_id"]: event for event in events}

    layers = process_deliveries(
        _bronze(schedule), known, timedelta(minutes=1), MANIFEST_HASH
    )

    assert len(layers["bronze"]) == len(schedule.deliveries)
    assert len(layers["parsed"]) == len(events)
    assert len(layers["quarantine"]) == 0
    assert len(layers["late_valid"]) == 1
    assert len(layers["silver"]) == len(events) - 1
    assert layers["counts"]["bronze_deliveries"] == (
        layers["counts"]["parse_valid"] + layers["counts"]["quarantine"]
    )


def test_retaining_watermark_covers_resolved_schedule_lateness() -> None:
    events = [_event(index) for index in range(4)]
    schedule = build_schedule(events, "delayed", seed=7, fault="delayed")

    with pytest.raises(ValueError, match="does not retain"):
        assert_retaining_watermark(schedule, timedelta(minutes=1))
    assert_retaining_watermark(schedule, timedelta(hours=4))


def test_duplicate_and_malformed_faults_conserve_layers() -> None:
    events = [_event(index) for index in range(3)]
    schedule = build_schedule(events, "combined", seed=7, fault="combined")
    known = {event["source_event_id"]: event for event in events}

    layers = process_deliveries(
        _bronze(schedule), known, timedelta(hours=4), MANIFEST_HASH
    )

    assert len(layers["silver"]) == len(events)
    assert len(layers["quarantine"]) == len(events)
    assert layers["counts"]["duplicate_valid"] == len(events)
    assert layers["counts"]["bronze_deliveries"] == (
        layers["counts"]["parse_valid"] + layers["counts"]["quarantine"]
    )

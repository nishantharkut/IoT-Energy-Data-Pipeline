from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from iot_energy_pipeline.replay import load_replay_manifest, make_manifest, write_json
from iot_energy_pipeline.schedule import build_schedule


def _event() -> dict:
    return {
        "source_event_id": "a" * 64,
        "event_time_utc": "2024-01-01T00:00:00Z",
        "meter_id": "meter-a",
        "scaled_value": 1,
    }


def test_replay_manifest_binds_snapshot_schedule_and_resolved_configuration(
    tmp_path: Path,
) -> None:
    schedule = build_schedule([_event()], "fixture-clean", seed=19, fault="clean")
    manifest = make_manifest(
        schedule,
        "b" * 64,
        requested_event_count=1,
        repetition=2,
    )
    path = tmp_path / "replay-manifest.json"

    write_json(manifest, path)
    saved = json.loads(path.read_text(encoding="utf-8"))

    assert saved == asdict(manifest)
    assert saved["schema_version"] == "replay-manifest-v1"
    assert saved["snapshot_hash"] == "b" * 64
    assert saved["schedule_hash"] == schedule.schedule_hash
    assert saved["fault"] == "clean"
    assert saved["expected_terminal_count"] == schedule.partition_count
    assert saved["requested_event_count"] == 1
    assert saved["repetition"] == 2


def test_replay_manifest_schema_is_closed() -> None:
    schema = json.loads(
        Path("schemas/replay-manifest-v1.schema.json").read_text(encoding="utf-8")
    )

    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(schema["properties"])


@pytest.mark.parametrize(
    "change",
    (
        {"fault": "silent_drop"},
        {"replay_run_id": "   "},
        {"maximum_event_time_lateness_seconds": -1},
        {"maximum_event_time_lateness_seconds": float("inf")},
    ),
)
def test_replay_manifest_loader_rejects_values_outside_the_closed_contract(
    tmp_path: Path, change: dict[str, object]
) -> None:
    schedule = build_schedule([_event()], "fixture-clean", seed=19, fault="clean")
    raw = asdict(make_manifest(schedule, "b" * 64, requested_event_count=1))
    raw.update(change)
    path = tmp_path / "replay-manifest.json"
    path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="replay manifest"):
        load_replay_manifest(path)

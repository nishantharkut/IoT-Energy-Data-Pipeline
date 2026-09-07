from __future__ import annotations

import json
from pathlib import Path

import pytest

from iot_energy_pipeline.machine_metadata import write_machine_metadata

SOFTWARE = {
    "python": "3.11.9",
    "docker": "27.0",
    "kafka": "apache/kafka:3.7.1",
    "spark": "3.5.1",
    "hadoop": "3.4.3",
}


def test_machine_metadata_is_explicit_deterministic_and_immutable(
    tmp_path: Path,
) -> None:
    output = tmp_path / "machine.json"
    value = write_machine_metadata(
        output=output,
        execution_context="designated_machine",
        software=SOFTWARE,
        captured_at_utc="2026-08-26T10:00:00Z",
        machine_facts={
            "hostname": "coursework-host",
            "operating_system": "Windows 11",
            "processor": "test-cpu",
            "logical_cpu_count": 8,
            "memory_bytes": 16_000_000_000,
        },
    )

    assert value["schema_version"] == "machine-metadata-v1"
    assert value["software"] == SOFTWARE
    assert json.loads(output.read_text("utf-8")) == value
    with pytest.raises(FileExistsError, match="immutable machine metadata"):
        write_machine_metadata(
            output=output,
            execution_context="designated_machine",
            software=SOFTWARE,
        )


def test_machine_metadata_rejects_incomplete_software_inventory(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="software inventory"):
        write_machine_metadata(
            output=tmp_path / "machine.json",
            execution_context="fixture",
            software={"python": "3.11"},
        )

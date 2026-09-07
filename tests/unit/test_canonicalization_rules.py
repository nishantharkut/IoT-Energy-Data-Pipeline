from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from iot_energy_pipeline.canonicalize import CanonicalizationRules


def _write_rules(tmp_path: Path, **changes: object) -> Path:
    layout = {
        "schema_version": "source-layout-v1",
        "evidence": "synthetic source evidence",
        "rules": [
            {
                "name": "fixture",
                "relative_path_pattern": "GUI_NO\\.(?P<meter_id>[A-Z0-9]+)\\.xlsx",
                "sheet_name": "Sheet1",
                "header_row": 1,
                "expected_columns": ["time", "number"],
                "timestamp_column": "time",
                "value_column": "number",
                "meter_id_column": None,
                "meter_id_path_group": "meter_id",
                "ignored_sheet_names": [],
                "evidence": "synthetic source evidence",
            }
        ],
    }
    layout_path = tmp_path / "layout.json"
    layout_path.write_text(json.dumps(layout), encoding="utf-8")
    config: dict[str, object] = {
        "schema_version": "canonicalization-rules-v1",
        "source_timezone": "Asia/Hong_Kong",
        "timezone_evidence": "dataset documentation states Hong Kong local time",
        "source_layout_file": "layout.json",
        "source_layout_sha256": hashlib.sha256(layout_path.read_bytes()).hexdigest(),
        "measurements": {
            "D0001": {
                "measurement_kind": "cumulative_energy",
                "unit": "kWh",
                "decimal_scale": 3,
                "evidence": "registered Brick unit and source documentation",
                "expected_brick_unit_suffix": "KiloW-HR",
            }
        },
    }
    config.update(changes)
    path = tmp_path / "canonicalization-rules.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def test_canonicalization_rules_load_closed_hash_bound_evidence(tmp_path: Path) -> None:
    rules = CanonicalizationRules.from_json(_write_rules(tmp_path))

    assert rules.source_timezone == "Asia/Hong_Kong"
    assert rules.source_layout.resolve("GUI_NO.D0001.xlsx").path_meter_id == "D0001"
    assert rules.measurements["D0001"].decimal_scale == 3
    assert rules.measurements["D0001"].measurement_kind == "cumulative_energy"


def test_canonicalization_rules_reject_unknown_fields(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown canonicalization-rules fields"):
        CanonicalizationRules.from_json(_write_rules(tmp_path, invented=True))


def test_canonicalization_rules_reject_replaced_layout(tmp_path: Path) -> None:
    path = _write_rules(tmp_path)
    layout_path = tmp_path / "layout.json"
    layout_path.write_text(layout_path.read_text("utf-8") + " ", encoding="utf-8")

    with pytest.raises(ValueError, match="source-layout hash mismatch"):
        CanonicalizationRules.from_json(path)

from __future__ import annotations

import json
from pathlib import Path

import pytest

from iot_energy_pipeline.source_layout import (
    SourceLayoutRules,
    WorkbookLayoutRule,
)


def _hkust_layout() -> SourceLayoutRules:
    return SourceLayoutRules(
        rules=(
            WorkbookLayoutRule(
                name="hkust-raw-meter-workbook",
                relative_path_pattern=(
                    r"(?:.*/)?GUI[._]NO[._](?P<meter_id>[A-Z][0-9]{4})\.xlsx"
                ),
                sheet_name="Sheet1",
                header_row=1,
                expected_columns=("time", "number"),
                timestamp_column="time",
                value_column="number",
                meter_id_path_group="meter_id",
                evidence="Dryad v5 description and authors' preprocessing commit",
            ),
        ),
        evidence="HKUST source-layout audit",
    )


def test_path_derived_meter_id_is_explicit_and_deterministic() -> None:
    layout = _hkust_layout()

    first = layout.resolve("Time-series data/GUI.NO.D0001.xlsx")
    second = layout.resolve("Time-series data/GUI_NO.D0001.xlsx")

    assert first.rule.name == "hkust-raw-meter-workbook"
    assert first.path_meter_id == second.path_meter_id == "D0001"
    assert layout.sha256() == layout.sha256()


def test_unknown_and_ambiguous_workbook_paths_fail_closed() -> None:
    layout = _hkust_layout()

    with pytest.raises(ValueError, match="no source-layout rule"):
        layout.resolve("Time-series data/unexpected.xlsx")

    ambiguous = SourceLayoutRules(
        rules=(
            layout.rules[0],
            WorkbookLayoutRule(
                name="second-match",
                relative_path_pattern=r".*/GUI\.NO\.(?P<id>D[0-9]{4})\.xlsx",
                sheet_name="Sheet1",
                header_row=1,
                expected_columns=("time", "number"),
                timestamp_column="time",
                value_column="number",
                meter_id_path_group="id",
                evidence="deliberately overlapping test rule",
            ),
        ),
        evidence="ambiguity test",
    )
    with pytest.raises(ValueError, match="multiple source-layout rules"):
        ambiguous.resolve("Time-series data/GUI.NO.D0001.xlsx")


def test_rule_requires_exactly_one_meter_identity_source() -> None:
    common = {
        "name": "invalid",
        "relative_path_pattern": r"readings\.xlsx",
        "sheet_name": "Readings",
        "header_row": 1,
        "expected_columns": ("timestamp", "meter_id", "value"),
        "timestamp_column": "timestamp",
        "value_column": "value",
        "evidence": "test",
    }

    with pytest.raises(ValueError, match="exactly one meter identity source"):
        WorkbookLayoutRule(**common)
    with pytest.raises(ValueError, match="exactly one meter identity source"):
        WorkbookLayoutRule(
            **common,
            meter_id_column="meter_id",
            meter_id_path_group="meter_id",
        )


def test_layout_json_round_trip_is_closed_and_byte_stable(tmp_path: Path) -> None:
    layout = _hkust_layout()
    output = tmp_path / "layout.json"

    layout.to_json(output)
    loaded = SourceLayoutRules.from_json(output)

    assert loaded.to_dict() == layout.to_dict()
    assert loaded.sha256() == layout.sha256()
    assert json.loads(output.read_text(encoding="utf-8"))["schema_version"] == (
        "source-layout-v1"
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    payload["unexpected"] = True
    output.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown source-layout fields"):
        SourceLayoutRules.from_json(output)

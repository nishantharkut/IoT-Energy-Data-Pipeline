from __future__ import annotations

import re
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "name,required_fragments",
    [
        ("meter", ("gold_source_events", "query_parameters", "meter_id")),
        (
            "building",
            ("gold_source_events", "query_parameters", "array_contains"),
        ),
        (
            "time_window",
            ("gold_source_events", "window_start_utc", "window_end_utc"),
        ),
        ("quality", ("gold_source_events", "quality_flags", "explode_outer")),
        (
            "exposure",
            ("decision_impact_scenarios", "normalized_rate", "gross_exposure"),
        ),
    ],
)
def test_declared_query_is_read_only_and_parameterized(
    name: str, required_fragments: tuple[str, ...]
) -> None:
    sql = (Path("queries") / f"{name}.sql").read_text(encoding="utf-8")
    normalized = " ".join(sql.lower().split())

    assert normalized.startswith("select") or normalized.startswith("with")
    assert normalized.endswith(";")
    assert all(fragment in normalized for fragment in required_fragments)
    assert not re.search(
        r"\b(insert|update|delete|merge|drop|alter|truncate|create|replace)\b",
        normalized,
    )

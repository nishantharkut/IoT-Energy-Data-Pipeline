from __future__ import annotations

import hashlib

import pytest

from iot_energy_pipeline.identity import (
    SourceLocator,
    assert_unique_source_ids,
    source_event_id,
)


def test_source_event_id_matches_declared_structural_contract() -> None:
    locator = SourceLocator("v5", "clean", "30min/M-1.xlsx", "Data", 17)
    expected = hashlib.sha256(b"v5|clean|30min/M-1.xlsx|Data|17").hexdigest()

    assert source_event_id(locator) == expected


def test_source_identity_distinguishes_physical_rows_with_same_values() -> None:
    first = SourceLocator("v5", "clean", "meter.xlsx", "Data", 2)
    second = SourceLocator("v5", "clean", "meter.xlsx", "Data", 3)

    assert source_event_id(first) != source_event_id(second)


@pytest.mark.parametrize(
    "locator",
    [
        SourceLocator("", "clean", "meter.xlsx", "Data", 2),
        SourceLocator("v5", "", "meter.xlsx", "Data", 2),
        SourceLocator("v5", "clean", "", "Data", 2),
        SourceLocator("v5", "clean", "meter.xlsx", "", 2),
        SourceLocator("v5", "clean", "meter.xlsx", "Data", 0),
        SourceLocator("v5|clean", "clean", "meter.xlsx", "Data", 2),
        SourceLocator("v5", "clean", "meter|name.xlsx", "Data", 2),
    ],
)
def test_source_locator_fails_closed_on_invalid_fields(locator: SourceLocator) -> None:
    with pytest.raises(ValueError):
        source_event_id(locator)


def test_duplicate_identifiers_are_rejected_with_both_locators() -> None:
    locator = SourceLocator("v5", "clean", "meter.xlsx", "Data", 2)

    with pytest.raises(ValueError, match="source_event_id collision"):
        assert_unique_source_ids([locator, locator])

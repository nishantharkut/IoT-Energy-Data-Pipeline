"""Structural identities for source rows and replay deliveries."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SourceLocator:
    """The immutable physical location of one row in a registered source."""

    dataset_version: str
    source_variant: str
    relative_path: str
    sheet: str
    physical_row: int

    def serialized(self) -> str:
        fields = (
            self.dataset_version,
            self.source_variant,
            self.relative_path,
            self.sheet,
        )
        if any(not isinstance(value, str) or not value.strip() for value in fields):
            raise ValueError("source locator text fields must be non-empty")
        if any("|" in value for value in fields):
            raise ValueError("source locator text fields cannot contain '|'")
        if (
            isinstance(self.physical_row, bool)
            or not isinstance(self.physical_row, int)
            or self.physical_row < 1
        ):
            raise ValueError("physical_row must be a positive integer")
        return "|".join((*fields, str(self.physical_row)))


def source_event_id(locator: SourceLocator) -> str:
    """Return the SHA-256 identity declared by the source-row contract."""

    return hashlib.sha256(locator.serialized().encode("utf-8")).hexdigest()


def assert_unique_source_ids(locators: Iterable[SourceLocator]) -> None:
    """Reject duplicate locators or a digest collision before snapshot output."""

    seen: dict[str, SourceLocator] = {}
    for locator in locators:
        identifier = source_event_id(locator)
        if identifier in seen:
            raise ValueError(
                "source_event_id collision: "
                f"{identifier} maps to {seen[identifier]!r} and {locator!r}"
            )
        seen[identifier] = locator

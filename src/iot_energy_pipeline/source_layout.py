"""Closed, evidence-bearing contracts for source workbook layouts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

_LAYOUT_FIELDS = {"schema_version", "evidence", "rules"}
_RULE_FIELDS = {
    "name",
    "relative_path_pattern",
    "sheet_name",
    "header_row",
    "expected_columns",
    "timestamp_column",
    "value_column",
    "meter_id_column",
    "meter_id_path_group",
    "ignored_sheet_names",
    "evidence",
}


def _closed_fields(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    unknown = sorted(set(value) - expected)
    if unknown:
        raise ValueError(f"unknown {label} fields: {', '.join(unknown)}")
    missing = sorted(expected - set(value))
    if missing:
        raise ValueError(f"missing {label} fields: {', '.join(missing)}")


def _canonical_json_bytes(value: Mapping[str, Any], *, pretty: bool) -> bytes:
    if pretty:
        text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    else:
        text = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    return (text + "\n").encode("utf-8")


@dataclass(frozen=True)
class WorkbookLayoutRule:
    """One exact workbook family and its meter-identity mechanism."""

    name: str
    relative_path_pattern: str
    sheet_name: str
    header_row: int
    expected_columns: tuple[str, ...]
    timestamp_column: str
    value_column: str
    evidence: str
    meter_id_column: str | None = None
    meter_id_path_group: str | None = None
    ignored_sheet_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        text_fields = {
            "name": self.name,
            "relative_path_pattern": self.relative_path_pattern,
            "sheet_name": self.sheet_name,
            "timestamp_column": self.timestamp_column,
            "value_column": self.value_column,
            "evidence": self.evidence,
        }
        blank = sorted(name for name, value in text_fields.items() if not value.strip())
        if blank:
            raise ValueError(f"blank source-layout fields: {', '.join(blank)}")
        if (
            isinstance(self.header_row, bool)
            or not isinstance(self.header_row, int)
            or self.header_row < 1
        ):
            raise ValueError("header_row must be a positive integer")
        if not self.expected_columns:
            raise ValueError("expected_columns must not be empty")
        if any(not column.strip() for column in self.expected_columns):
            raise ValueError("expected column names must be non-empty")
        folded_columns = [column.casefold() for column in self.expected_columns]
        if len(set(folded_columns)) != len(folded_columns):
            raise ValueError("expected column names must be unique case-insensitively")
        required_columns = {self.timestamp_column, self.value_column}
        if self.meter_id_column is not None:
            required_columns.add(self.meter_id_column)
        missing_columns = sorted(required_columns - set(self.expected_columns))
        if missing_columns:
            raise ValueError(
                "layout columns are absent from expected_columns: "
                + ", ".join(missing_columns)
            )
        identity_count = sum(
            item is not None
            for item in (self.meter_id_column, self.meter_id_path_group)
        )
        if identity_count != 1:
            raise ValueError("exactly one meter identity source is required")
        if self.meter_id_column is not None and not self.meter_id_column.strip():
            raise ValueError("meter_id_column must be non-empty when supplied")
        if (
            self.meter_id_path_group is not None
            and not self.meter_id_path_group.strip()
        ):
            raise ValueError("meter_id_path_group must be non-empty when supplied")
        if len(set(self.ignored_sheet_names)) != len(self.ignored_sheet_names):
            raise ValueError("ignored sheet names must be unique")
        if self.sheet_name in self.ignored_sheet_names:
            raise ValueError("the data sheet cannot also be ignored")
        if any(not name.strip() for name in self.ignored_sheet_names):
            raise ValueError("ignored sheet names must be non-empty")
        try:
            compiled = re.compile(self.relative_path_pattern)
        except re.error as exc:
            raise ValueError(f"invalid relative_path_pattern: {exc}") from exc
        if (
            self.meter_id_path_group is not None
            and self.meter_id_path_group not in compiled.groupindex
        ):
            raise ValueError(
                "meter_id_path_group is not a named group in relative_path_pattern"
            )

    def match(self, relative_path: str) -> re.Match[str] | None:
        """Full-match a registered POSIX relative path."""

        return re.fullmatch(self.relative_path_pattern, relative_path)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-ready projection."""

        value = asdict(self)
        value["expected_columns"] = list(self.expected_columns)
        value["ignored_sheet_names"] = list(self.ignored_sheet_names)
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkbookLayoutRule":
        """Load a rule from a closed JSON object."""

        _closed_fields(value, _RULE_FIELDS, "workbook-layout rule")
        return cls(
            name=str(value["name"]),
            relative_path_pattern=str(value["relative_path_pattern"]),
            sheet_name=str(value["sheet_name"]),
            header_row=value["header_row"],
            expected_columns=tuple(value["expected_columns"]),
            timestamp_column=str(value["timestamp_column"]),
            value_column=str(value["value_column"]),
            meter_id_column=(
                None
                if value["meter_id_column"] is None
                else str(value["meter_id_column"])
            ),
            meter_id_path_group=(
                None
                if value["meter_id_path_group"] is None
                else str(value["meter_id_path_group"])
            ),
            ignored_sheet_names=tuple(value["ignored_sheet_names"]),
            evidence=str(value["evidence"]),
        )


@dataclass(frozen=True)
class ResolvedWorkbookLayout:
    """The unique rule match for one registered workbook."""

    rule: WorkbookLayoutRule
    path_meter_id: str | None


@dataclass(frozen=True)
class SourceLayoutRules:
    """Closed collection that must cover every registered workbook exactly once."""

    rules: tuple[WorkbookLayoutRule, ...]
    evidence: str
    schema_version: str = "source-layout-v1"

    def __post_init__(self) -> None:
        if self.schema_version != "source-layout-v1":
            raise ValueError("unsupported source-layout schema_version")
        if not self.evidence.strip():
            raise ValueError("source-layout evidence must be non-empty")
        if not self.rules:
            raise ValueError("at least one source-layout rule is required")
        names = [rule.name for rule in self.rules]
        if len(set(names)) != len(names):
            raise ValueError("source-layout rule names must be unique")

    def resolve(self, relative_path: str) -> ResolvedWorkbookLayout:
        """Resolve one path, rejecting unknown and overlapping rule matches."""

        matches: list[tuple[WorkbookLayoutRule, re.Match[str]]] = []
        for rule in self.rules:
            match = rule.match(relative_path)
            if match is not None:
                matches.append((rule, match))
        if not matches:
            raise ValueError(f"no source-layout rule for workbook {relative_path!r}")
        if len(matches) > 1:
            names = ", ".join(sorted(rule.name for rule, _ in matches))
            raise ValueError(
                f"multiple source-layout rules for workbook {relative_path!r}: {names}"
            )
        rule, match = matches[0]
        path_meter_id = None
        if rule.meter_id_path_group is not None:
            path_meter_id = match.group(rule.meter_id_path_group).strip()
            if not path_meter_id:
                raise ValueError(
                    f"empty meter ID derived from workbook path {relative_path!r}"
                )
        return ResolvedWorkbookLayout(rule=rule, path_meter_id=path_meter_id)

    def to_dict(self) -> dict[str, Any]:
        """Return the deterministic persisted layout contract."""

        return {
            "schema_version": self.schema_version,
            "evidence": self.evidence,
            "rules": [rule.to_dict() for rule in self.rules],
        }

    def sha256(self) -> str:
        """Hash the semantic JSON representation of this contract."""

        return hashlib.sha256(
            _canonical_json_bytes(self.to_dict(), pretty=False)
        ).hexdigest()

    def to_json(self, path: Path) -> None:
        """Persist the contract as canonical, human-readable JSON."""

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_canonical_json_bytes(self.to_dict(), pretty=True))

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "SourceLayoutRules":
        """Load a closed source-layout object."""

        _closed_fields(value, _LAYOUT_FIELDS, "source-layout")
        rules_value = value["rules"]
        if not isinstance(rules_value, list):
            raise ValueError("source-layout rules must be a list")
        return cls(
            rules=tuple(WorkbookLayoutRule.from_dict(item) for item in rules_value),
            evidence=str(value["evidence"]),
            schema_version=str(value["schema_version"]),
        )

    @classmethod
    def from_json(cls, path: Path) -> "SourceLayoutRules":
        """Read and validate a UTF-8 JSON source-layout contract."""

        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid source-layout JSON: {path}") from exc
        if not isinstance(value, dict):
            raise ValueError("source-layout JSON root must be an object")
        return cls.from_dict(value)

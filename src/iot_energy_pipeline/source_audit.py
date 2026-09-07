"""Deterministic, non-transforming telemetry audits over registered workbooks."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections import Counter
from dataclasses import asdict
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .manifests import DatasetManifest
from .source_layout import SourceLayoutRules
from .workbook_source import (
    WorkbookSourceRow,
    WorkbookSourceStart,
    iter_workbook_source_items,
)

_COUNT_FIELDS = (
    "source_row_count",
    "empty_source_row_count",
    "missing_meter_id_count",
    "missing_timestamp_count",
    "invalid_timestamp_count",
    "naive_timestamp_count",
    "aware_timestamp_count",
    "duplicate_timestamp_count",
    "out_of_order_timestamp_count",
    "missing_numeric_count",
    "invalid_numeric_count",
    "non_finite_numeric_count",
    "finite_numeric_count",
    "zero_numeric_count",
    "negative_numeric_count",
    "ordered_numeric_decrease_count",
)


def _canonical_json_bytes(value: Any) -> bytes:
    text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    return (text + "\n").encode("utf-8")


def _manifest_projection(manifest: DatasetManifest) -> dict[str, Any]:
    return {
        "dataset_version": manifest.dataset_version,
        "source_variant": manifest.source_variant,
        "files": [asdict(file) for file in manifest.files],
    }


def _missing(value: object) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _display_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    return str(value).strip()


def _parse_timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min)
    if isinstance(value, bool) or isinstance(value, time):
        raise ValueError("value is not a complete timestamp")
    text = _display_text(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("value is not an ISO-compatible timestamp") from exc


def _timestamp_kind(value: datetime) -> str:
    return "aware" if value.utcoffset() is not None else "naive"


def _normalized_timestamp(value: datetime) -> datetime:
    if _timestamp_kind(value) == "aware":
        return value.astimezone(timezone.utc)
    return value


def _parse_decimal(value: object) -> Decimal:
    if isinstance(value, bool):
        raise InvalidOperation("boolean is not a numeric reading")
    return Decimal(_display_text(value))


def _decimal_scale(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):
        raise InvalidOperation("non-finite decimal has no numeric scale")
    return max(0, -exponent)


def _seconds_key(seconds: float) -> str:
    if seconds.is_integer():
        return str(int(seconds))
    return format(Decimal(str(seconds)).normalize(), "f")


class _WorkbookAudit:
    def __init__(
        self, start: WorkbookSourceStart, connection: sqlite3.Connection
    ) -> None:
        self.relative_path = start.relative_path
        self.sheet_name = start.sheet_name
        self.layout_rule = start.layout_rule
        self.connection = connection
        self.counts: Counter[str] = Counter()
        self.meter_ids: set[str] = set()
        if start.path_meter_id is not None:
            self.meter_ids.add(start.path_meter_id)
        self.timestamp_batch: list[tuple[str, str, str]] = []
        self.previous_timestamp: dict[tuple[str, str], datetime] = {}
        self.previous_numeric: dict[tuple[str, str], tuple[datetime, Decimal]] = {}
        self.naive_minimum: datetime | None = None
        self.naive_maximum: datetime | None = None
        self.aware_minimum: datetime | None = None
        self.aware_maximum: datetime | None = None
        self.decimal_scales: Counter[int] = Counter()

    def _flush_timestamps(self) -> None:
        if not self.timestamp_batch:
            return
        self.connection.executemany(
            "INSERT INTO timestamps VALUES (?, ?, ?)", self.timestamp_batch
        )
        self.timestamp_batch.clear()

    def add(self, row: WorkbookSourceRow) -> None:
        if row.locator.relative_path != self.relative_path:
            raise ValueError("workbook audit received a row from another workbook")
        self.counts["source_row_count"] += 1
        if all(_missing(value) for value in row.row_values):
            self.counts["empty_source_row_count"] += 1

        meter_id: str | None = None
        if _missing(row.meter_id_value):
            self.counts["missing_meter_id_count"] += 1
        else:
            meter_id = _display_text(row.meter_id_value)
            self.meter_ids.add(meter_id)

        parsed_timestamp: datetime | None = None
        timestamp_kind: str | None = None
        if _missing(row.timestamp_value):
            self.counts["missing_timestamp_count"] += 1
        else:
            try:
                parsed_timestamp = _normalized_timestamp(
                    _parse_timestamp(row.timestamp_value)
                )
            except ValueError:
                self.counts["invalid_timestamp_count"] += 1
            else:
                timestamp_kind = _timestamp_kind(parsed_timestamp)
                self.counts[f"{timestamp_kind}_timestamp_count"] += 1
                if timestamp_kind == "aware":
                    self.aware_minimum = (
                        parsed_timestamp
                        if self.aware_minimum is None
                        else min(self.aware_minimum, parsed_timestamp)
                    )
                    self.aware_maximum = (
                        parsed_timestamp
                        if self.aware_maximum is None
                        else max(self.aware_maximum, parsed_timestamp)
                    )
                else:
                    self.naive_minimum = (
                        parsed_timestamp
                        if self.naive_minimum is None
                        else min(self.naive_minimum, parsed_timestamp)
                    )
                    self.naive_maximum = (
                        parsed_timestamp
                        if self.naive_maximum is None
                        else max(self.naive_maximum, parsed_timestamp)
                    )
                if meter_id is not None:
                    series_key = (meter_id, timestamp_kind)
                    self.timestamp_batch.append(
                        (
                            meter_id,
                            timestamp_kind,
                            parsed_timestamp.isoformat(timespec="microseconds"),
                        )
                    )
                    if len(self.timestamp_batch) >= 1000:
                        self._flush_timestamps()
                    previous = self.previous_timestamp.get(series_key)
                    if previous is not None and parsed_timestamp < previous:
                        self.counts["out_of_order_timestamp_count"] += 1
                    self.previous_timestamp[series_key] = parsed_timestamp

        parsed_numeric: Decimal | None = None
        if _missing(row.numeric_value):
            self.counts["missing_numeric_count"] += 1
        else:
            try:
                parsed_numeric = _parse_decimal(row.numeric_value)
            except (InvalidOperation, ValueError):
                self.counts["invalid_numeric_count"] += 1
            else:
                if not parsed_numeric.is_finite():
                    self.counts["non_finite_numeric_count"] += 1
                    parsed_numeric = None
                else:
                    self.counts["finite_numeric_count"] += 1
                    self.decimal_scales[_decimal_scale(parsed_numeric)] += 1
                    if parsed_numeric.is_zero():
                        self.counts["zero_numeric_count"] += 1
                    if parsed_numeric < 0:
                        self.counts["negative_numeric_count"] += 1

        if (
            meter_id is not None
            and parsed_timestamp is not None
            and timestamp_kind is not None
            and parsed_numeric is not None
        ):
            series_key = (meter_id, timestamp_kind)
            previous_numeric = self.previous_numeric.get(series_key)
            if previous_numeric is None:
                self.previous_numeric[series_key] = (
                    parsed_timestamp,
                    parsed_numeric,
                )
            elif parsed_timestamp > previous_numeric[0]:
                if parsed_numeric < previous_numeric[1]:
                    self.counts["ordered_numeric_decrease_count"] += 1
                self.previous_numeric[series_key] = (
                    parsed_timestamp,
                    parsed_numeric,
                )

    def finish(self) -> dict[str, Any]:
        self._flush_timestamps()
        self.connection.commit()
        cadence: Counter[str] = Counter()
        previous_series: tuple[str, str] | None = None
        previous_timestamp: datetime | None = None
        duplicate_count = 0
        for meter_id, kind, timestamp_text, occurrence_count in self.connection.execute(
            "SELECT meter_id, timestamp_kind, timestamp_text, COUNT(*) "
            "FROM timestamps GROUP BY meter_id, timestamp_kind, timestamp_text "
            "ORDER BY meter_id, timestamp_kind, timestamp_text"
        ):
            duplicate_count += int(occurrence_count) - 1
            series = (str(meter_id), str(kind))
            timestamp = datetime.fromisoformat(str(timestamp_text))
            if previous_series == series and previous_timestamp is not None:
                seconds = (timestamp - previous_timestamp).total_seconds()
                if seconds > 0:
                    cadence[_seconds_key(seconds)] += 1
            previous_series = series
            previous_timestamp = timestamp
        self.counts["duplicate_timestamp_count"] = duplicate_count
        self.connection.execute("DELETE FROM timestamps")
        self.connection.commit()
        return {
            "relative_path": self.relative_path,
            "sheet_name": self.sheet_name,
            "layout_rule": self.layout_rule.name,
            "empty_workbook": self.counts["source_row_count"] == 0,
            "meter_ids": sorted(self.meter_ids),
            **{field: self.counts[field] for field in _COUNT_FIELDS},
            "cadence_seconds_histogram": dict(sorted(cadence.items())),
            "decimal_scale_histogram": {
                str(scale): count
                for scale, count in sorted(self.decimal_scales.items())
            },
            "timestamp_bounds": {
                "naive": {
                    "minimum": (
                        self.naive_minimum.isoformat()
                        if self.naive_minimum is not None
                        else None
                    ),
                    "maximum": (
                        self.naive_maximum.isoformat()
                        if self.naive_maximum is not None
                        else None
                    ),
                },
                "aware_utc": {
                    "minimum": (
                        self.aware_minimum.isoformat().replace("+00:00", "Z")
                        if self.aware_minimum is not None
                        else None
                    ),
                    "maximum": (
                        self.aware_maximum.isoformat().replace("+00:00", "Z")
                        if self.aware_maximum is not None
                        else None
                    ),
                },
            },
        }


def _write_atomic_new(path: Path, payload: bytes) -> None:
    if path.exists():
        raise FileExistsError(f"immutable source audit already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
        if path.exists():
            raise FileExistsError(f"immutable source audit already exists: {path}")
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def audit_source_telemetry(
    manifest: DatasetManifest,
    source_layout: SourceLayoutRules,
    output: Path,
) -> dict[str, Any]:
    """Audit timestamps and numeric cells without assigning source semantics."""

    if output.exists():
        raise FileExistsError(f"immutable source audit already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, database_name = tempfile.mkstemp(
        dir=output.parent, prefix=".source-audit.", suffix=".sqlite3"
    )
    os.close(descriptor)
    database_path = Path(database_name)
    connection = sqlite3.connect(database_path)
    workbooks: list[dict[str, Any]] = []
    current: _WorkbookAudit | None = None
    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(
            "CREATE TABLE timestamps (meter_id TEXT, timestamp_kind TEXT, "
            "timestamp_text TEXT)"
        )
        for item in iter_workbook_source_items(manifest, source_layout):
            if isinstance(item, WorkbookSourceStart):
                if current is not None:
                    workbooks.append(current.finish())
                current = _WorkbookAudit(item, connection)
                continue
            if current is None or item.locator.relative_path != current.relative_path:
                raise ValueError("workbook row appeared without its declared boundary")
            current.add(item)
        if current is not None:
            workbooks.append(current.finish())
    finally:
        connection.close()
        try:
            database_path.unlink()
        except FileNotFoundError:
            pass

    all_meter_ids = {
        meter_id for workbook in workbooks for meter_id in workbook["meter_ids"]
    }
    totals = {
        "workbook_count": len(workbooks),
        "empty_workbook_count": sum(
            workbook["empty_workbook"] for workbook in workbooks
        ),
        "meter_count": len(all_meter_ids),
        **{
            field: sum(workbook[field] for workbook in workbooks)
            for field in _COUNT_FIELDS
        },
    }
    projection = _manifest_projection(manifest)
    report: dict[str, Any] = {
        "schema_version": "source-telemetry-audit-v1",
        "status": "complete",
        "dataset_version": manifest.dataset_version,
        "source_variant": manifest.source_variant,
        "dataset_manifest_sha256": hashlib.sha256(
            _canonical_json_bytes(projection)
        ).hexdigest(),
        "source_layout_sha256": source_layout.sha256(),
        "source_layout": source_layout.to_dict(),
        "timezone_assignment": "UNASSIGNED",
        "measurement_semantics_assignment": "UNASSIGNED",
        "totals": totals,
        "workbooks": workbooks,
    }
    _write_atomic_new(output, _canonical_json_bytes(report))
    return report

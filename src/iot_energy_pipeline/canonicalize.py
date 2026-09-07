"""Build provenance-bound canonical NDJSON and Parquet snapshots."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .brick import BrickMetadata, load_brick_metadata
from .contracts import (
    CanonicalEvent,
    CanonicalMeteredEntity,
    canonicalize_time,
    to_scaled_int,
)
from .identity import SourceLocator, assert_unique_source_ids, source_event_id
from .manifests import DatasetManifest
from .source_layout import SourceLayoutRules
from .workbook_source import iter_workbook_source_rows


@dataclass(frozen=True)
class MeasurementRule:
    """Evidence-backed representation contract for one meter."""

    measurement_kind: str
    unit: str
    decimal_scale: int
    evidence: str
    expected_brick_unit_suffix: str | None = None

    def validate(self) -> None:
        if not self.measurement_kind.strip() or not self.unit.strip():
            raise ValueError("measurement kind and unit must be non-empty")
        if (
            isinstance(self.decimal_scale, bool)
            or not isinstance(self.decimal_scale, int)
            or self.decimal_scale < 0
        ):
            raise ValueError("decimal scale must be a non-negative integer")
        if not self.evidence.strip():
            raise ValueError("measurement evidence must be non-empty")


@dataclass(frozen=True)
class CanonicalizationRules:
    """Explicit timezone and per-meter contracts approved after profiling."""

    source_timezone: str
    timezone_evidence: str
    source_layout: SourceLayoutRules
    measurements: Mapping[str, MeasurementRule]

    def __post_init__(self) -> None:
        if not self.source_timezone.strip() or not self.timezone_evidence.strip():
            raise ValueError("timezone and timezone evidence must be non-empty")
        try:
            ZoneInfo(self.source_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(
                f"unknown source timezone: {self.source_timezone}"
            ) from exc
        copied: dict[str, MeasurementRule] = {}
        for meter_id, rule in self.measurements.items():
            if not meter_id.strip():
                raise ValueError("measurement meter IDs must be non-empty")
            rule.validate()
            copied[meter_id] = rule
        object.__setattr__(self, "measurements", MappingProxyType(copied))

    def with_measurement(
        self, meter_id: str, rule: MeasurementRule
    ) -> "CanonicalizationRules":
        updated = dict(self.measurements)
        updated[meter_id] = rule
        return CanonicalizationRules(
            self.source_timezone,
            self.timezone_evidence,
            self.source_layout,
            updated,
        )

    @classmethod
    def from_json(cls, path: Path) -> "CanonicalizationRules":
        """Load a closed, hash-bound evidence contract from UTF-8 JSON."""

        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid canonicalization-rules JSON: {path}") from exc
        if not isinstance(value, dict):
            raise ValueError("canonicalization-rules JSON root must be an object")
        fields = {
            "schema_version",
            "source_timezone",
            "timezone_evidence",
            "source_layout_file",
            "source_layout_sha256",
            "measurements",
        }
        unknown = sorted(set(value) - fields)
        missing = sorted(fields - set(value))
        if unknown:
            raise ValueError(
                "unknown canonicalization-rules fields: " + ", ".join(unknown)
            )
        if missing:
            raise ValueError(
                "missing canonicalization-rules fields: " + ", ".join(missing)
            )
        if value["schema_version"] != "canonicalization-rules-v1":
            raise ValueError("unsupported canonicalization-rules schema_version")
        for field in (
            "source_timezone",
            "timezone_evidence",
            "source_layout_file",
            "source_layout_sha256",
        ):
            if not isinstance(value[field], str) or not value[field].strip():
                raise ValueError(f"canonicalization-rules {field} must be non-empty")

        relative = str(value["source_layout_file"])
        if "\\" in relative:
            raise ValueError("source_layout_file must be a portable relative path")
        posix = PurePosixPath(relative)
        if posix.is_absolute() or any(part in {"", ".", ".."} for part in posix.parts):
            raise ValueError("source_layout_file must be a portable relative path")
        layout_path = path.parent.joinpath(*posix.parts)
        try:
            layout_path.resolve(strict=True).relative_to(
                path.parent.resolve(strict=True)
            )
        except (OSError, ValueError) as exc:
            raise ValueError(
                "source_layout_file must resolve below the rules directory"
            ) from exc
        expected_hash = value["source_layout_sha256"]
        if (
            len(expected_hash) != 64
            or any(character not in "0123456789abcdef" for character in expected_hash)
            or _sha256(layout_path) != expected_hash
        ):
            raise ValueError("source-layout hash mismatch")

        raw_measurements = value["measurements"]
        if not isinstance(raw_measurements, dict) or not raw_measurements:
            raise ValueError("measurements must be a non-empty object")
        measurement_fields = {
            "measurement_kind",
            "unit",
            "decimal_scale",
            "evidence",
            "expected_brick_unit_suffix",
        }
        measurements: dict[str, MeasurementRule] = {}
        for meter_id, raw_rule in raw_measurements.items():
            if not isinstance(meter_id, str) or not meter_id.strip():
                raise ValueError("measurement meter IDs must be non-empty")
            if not isinstance(raw_rule, dict):
                raise ValueError(f"measurement rule for {meter_id!r} must be an object")
            unknown_rule = sorted(set(raw_rule) - measurement_fields)
            missing_rule = sorted(measurement_fields - set(raw_rule))
            if unknown_rule:
                raise ValueError(
                    f"unknown measurement fields for {meter_id!r}: "
                    + ", ".join(unknown_rule)
                )
            if missing_rule:
                raise ValueError(
                    f"missing measurement fields for {meter_id!r}: "
                    + ", ".join(missing_rule)
                )
            for field in ("measurement_kind", "unit", "evidence"):
                if not isinstance(raw_rule[field], str) or not raw_rule[field].strip():
                    raise ValueError(
                        f"measurement {field} for {meter_id!r} must be non-empty"
                    )
            suffix = raw_rule["expected_brick_unit_suffix"]
            if suffix is not None and (
                not isinstance(suffix, str) or not suffix.strip()
            ):
                raise ValueError(
                    "expected_brick_unit_suffix must be null or a non-empty string"
                )
            measurements[meter_id] = MeasurementRule(
                measurement_kind=raw_rule["measurement_kind"],
                unit=raw_rule["unit"],
                decimal_scale=raw_rule["decimal_scale"],
                evidence=raw_rule["evidence"],
                expected_brick_unit_suffix=suffix,
            )
        return cls(
            source_timezone=value["source_timezone"],
            timezone_evidence=value["timezone_evidence"],
            source_layout=SourceLayoutRules.from_json(layout_path),
            measurements=measurements,
        )


@dataclass(frozen=True)
class SnapshotResult:
    events: tuple[dict[str, Any], ...] | None
    source_quality: tuple[dict[str, Any], ...] | None
    manifest: dict[str, Any]

    @property
    def event_count(self) -> int:
        return int(self.manifest["artifacts"]["canonical_ndjson"]["record_count"])

    @property
    def source_quality_count(self) -> int:
        return int(self.manifest["artifacts"]["source_quality"]["record_count"])


_SNAPSHOT_RESULT_MATERIALIZATION_LIMIT = 10_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        text = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True)
    else:
        text = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
    return (text + "\n").encode("utf-8")


def _manifest_projection(manifest: DatasetManifest) -> dict[str, Any]:
    return {
        "dataset_version": manifest.dataset_version,
        "source_variant": manifest.source_variant,
        "files": [asdict(file) for file in manifest.files],
    }


def _display_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, time)):
        return value.isoformat()
    return str(value).strip()


def _quality_row(
    locator: SourceLocator,
    category: str,
    *,
    timestamp: object,
    meter_id: object,
    value: object,
    detail: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "dataset_version": locator.dataset_version,
        "source_variant": locator.source_variant,
        "source_relative_path": locator.relative_path,
        "workbook_sheet": locator.sheet,
        "source_row_number": locator.physical_row,
        "source_event_id": source_event_id(locator),
        "original_timestamp_text": (
            None if timestamp is None else _display_text(timestamp)
        ),
        "meter_id": None if meter_id is None else _display_text(meter_id),
        "original_numeric_text": None if value is None else _display_text(value),
        "detail": detail,
    }


def _registered_brick_path(manifest: DatasetManifest) -> Path:
    brick_files = [file for file in manifest.files if file.media_kind == "brick_ttl"]
    if len(brick_files) != 1:
        raise ValueError(
            "canonicalization requires exactly one registered Brick TTL file"
        )
    if manifest._root is None:
        raise ValueError("manifest must retain a verified source root")
    return manifest._root / brick_files[0].relative_path


def _canonicalize(
    manifest: DatasetManifest,
    rules: CanonicalizationRules,
    *,
    event_sink: Callable[[dict[str, Any]], None] | None = None,
    quality_sink: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], BrickMetadata]:
    if manifest._root is None:
        raise ValueError("manifest must retain a verified source root")
    brick = load_brick_metadata(_registered_brick_path(manifest))
    source_zone = ZoneInfo(rules.source_timezone)
    events: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    locators: list[SourceLocator] = []

    def reject(row: dict[str, Any]) -> None:
        if quality_sink is None:
            rejected.append(row)
        else:
            quality_sink(row)

    def accept(row: dict[str, Any], locator: SourceLocator) -> None:
        if event_sink is None:
            events.append(row)
            locators.append(locator)
        else:
            event_sink(row)

    for source_row in iter_workbook_source_rows(manifest, rules.source_layout):
        locator = source_row.locator
        timestamp = source_row.timestamp_value
        meter_value = source_row.meter_id_value
        numeric = source_row.numeric_value
        if timestamp is None:
            reject(
                _quality_row(
                    locator,
                    "missing_timestamp",
                    timestamp=timestamp,
                    meter_id=meter_value,
                    value=numeric,
                    detail="timestamp cell is null",
                )
            )
            continue
        if meter_value is None or not _display_text(meter_value):
            reject(
                _quality_row(
                    locator,
                    "missing_meter_id",
                    timestamp=timestamp,
                    meter_id=meter_value,
                    value=numeric,
                    detail="meter identifier cell is null or blank",
                )
            )
            continue
        meter_id = _display_text(meter_value)
        if numeric is None or (isinstance(numeric, str) and not numeric.strip()):
            reject(
                _quality_row(
                    locator,
                    "missing_numeric_value",
                    timestamp=timestamp,
                    meter_id=meter_id,
                    value=numeric,
                    detail="numeric cell is null or blank",
                )
            )
            continue
        rule = rules.measurements.get(meter_id)
        if rule is None:
            raise ValueError(
                f"no evidence-backed measurement contract for {meter_id!r}"
            )
        timestamp_text = _display_text(timestamp)
        numeric_text = _display_text(numeric)
        try:
            event_time = canonicalize_time(timestamp_text, source_zone)
        except ValueError as exc:
            reject(
                _quality_row(
                    locator,
                    "invalid_timestamp",
                    timestamp=timestamp,
                    meter_id=meter_id,
                    value=numeric,
                    detail=str(exc),
                )
            )
            continue
        try:
            scaled_value = to_scaled_int(numeric_text, rule.decimal_scale)
        except (ValueError, OverflowError) as exc:
            category = (
                "numeric_overflow"
                if isinstance(exc, OverflowError)
                else "invalid_numeric_value"
            )
            reject(
                _quality_row(
                    locator,
                    category,
                    timestamp=timestamp,
                    meter_id=meter_id,
                    value=numeric,
                    detail=str(exc),
                )
            )
            continue

        join = brick.join_by_meter_id.get(meter_id)
        if rule.expected_brick_unit_suffix is not None and (
            join is None
            or join.unit_uri is None
            or not join.unit_uri.endswith(rule.expected_brick_unit_suffix)
        ):
            raise ValueError(
                f"Brick unit contract mismatch for {meter_id!r}: "
                f"expected suffix {rule.expected_brick_unit_suffix!r}, "
                f"got {join.unit_uri if join else None!r}"
            )
        flags: tuple[str, ...]
        if join is None:
            flags = ("brick_unmatched",)
        else:
            flags = tuple(
                sorted(
                    name
                    for missing, name in (
                        (not join.building_ids, "brick_building_unresolved"),
                        (not join.zone_ids, "brick_zone_unresolved"),
                        (
                            not join.metered_entities,
                            "brick_metered_entity_unresolved",
                        ),
                        (join.unit_uri is None, "brick_unit_missing"),
                    )
                    if missing
                )
            )
        event = CanonicalEvent(
            schema_version="canonical-event-v1",
            dataset_version=manifest.dataset_version,
            source_variant=manifest.source_variant,
            source_event_id=source_event_id(locator),
            source_relative_path=locator.relative_path,
            workbook_sheet=locator.sheet,
            source_row_number=locator.physical_row,
            meter_id=meter_id,
            brick_entity_id=(join.brick_entity_id if join else None),
            brick_building_ids=(join.building_ids if join else ()),
            brick_zone_ids=(join.zone_ids if join else ()),
            brick_metered_entities=(
                tuple(
                    CanonicalMeteredEntity(entity.entity_id, entity.type_uris)
                    for entity in join.metered_entities
                )
                if join
                else ()
            ),
            brick_unit_uri=(join.unit_uri if join else None),
            brick_usage_type=(join.usage_type if join else None),
            original_timestamp_text=timestamp_text,
            source_timezone=rules.source_timezone,
            event_time_utc=event_time.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            original_numeric_text=numeric_text,
            scaled_value=scaled_value,
            decimal_scale=rule.decimal_scale,
            measurement_kind=rule.measurement_kind,
            unit=rule.unit,
            quality_flags=flags,
        )
        accept(event.to_dict(), locator)

    if event_sink is None:
        assert_unique_source_ids(locators)
        events.sort(key=lambda event: event["source_event_id"])
    if quality_sink is None:
        rejected.sort(
            key=lambda row: (
                row["source_relative_path"],
                row["workbook_sheet"],
                row["source_row_number"],
                row["category"],
            )
        )
    return events, rejected, brick


def canonicalize_workbook(
    manifest: DatasetManifest,
    rules: CanonicalizationRules | None = None,
) -> list[dict[str, Any]]:
    """Return canonical records without writing a snapshot."""

    if rules is None:
        raise ValueError("explicit canonicalization rules are required")
    events, _, _ = _canonicalize(manifest, rules)
    return events


def _write_ndjson(records: list[dict[str, Any]], path: Path) -> None:
    with path.open("wb") as stream:
        for record in records:
            stream.write(_canonical_json_bytes(record))


def _parquet_modules_and_schema() -> tuple[Any, Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - dependency gate
        raise RuntimeError("dataset extra is required to write Parquet") from exc

    schema = pa.schema(
        [
            ("schema_version", pa.string()),
            ("dataset_version", pa.string()),
            ("source_variant", pa.string()),
            ("source_event_id", pa.string()),
            ("source_relative_path", pa.string()),
            ("workbook_sheet", pa.string()),
            ("source_row_number", pa.int64()),
            ("meter_id", pa.string()),
            ("brick_entity_id", pa.string()),
            ("brick_building_ids", pa.list_(pa.string())),
            ("brick_zone_ids", pa.list_(pa.string())),
            (
                "brick_metered_entities",
                pa.list_(
                    pa.struct(
                        [
                            ("entity_id", pa.string()),
                            ("type_uris", pa.list_(pa.string())),
                        ]
                    )
                ),
            ),
            ("brick_unit_uri", pa.string()),
            ("brick_usage_type", pa.string()),
            ("original_timestamp_text", pa.string()),
            ("source_timezone", pa.string()),
            ("event_time_utc", pa.string()),
            ("original_numeric_text", pa.string()),
            ("scaled_value", pa.int64()),
            ("decimal_scale", pa.int32()),
            ("measurement_kind", pa.string()),
            ("unit", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
        ]
    )
    return pa, pq, schema


def _write_parquet(records: list[dict[str, Any]], path: Path) -> None:
    pa, pq, schema = _parquet_modules_and_schema()
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
    )


def _write_parquet_from_database(
    connection: sqlite3.Connection, path: Path, record_count: int
) -> None:
    pa, pq, schema = _parquet_modules_and_schema()
    if record_count == 0:
        _write_parquet([], path)
        return
    writer = pq.ParquetWriter(
        path,
        schema,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
    )
    try:
        batch: list[dict[str, Any]] = []
        for (payload,) in connection.execute(
            "SELECT payload FROM events ORDER BY source_event_id"
        ):
            value = json.loads(str(payload))
            if not isinstance(value, dict):
                raise ValueError("canonical event store contains an invalid payload")
            batch.append(value)
            if len(batch) >= 10_000:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch.clear()
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
    finally:
        writer.close()


def _write_payload_query(
    connection: sqlite3.Connection, query: str, path: Path
) -> None:
    with path.open("xb") as stream:
        for (payload,) in connection.execute(query):
            stream.write((str(payload) + "\n").encode("utf-8"))


def build_snapshot(
    manifest: DatasetManifest,
    output_dir: Path,
    rules: CanonicalizationRules,
) -> SnapshotResult:
    """Create a complete immutable snapshot directory atomically."""

    if output_dir.exists():
        raise FileExistsError(f"immutable snapshot already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    database_path = stage / ".canonical-sort.sqlite3"
    connection = sqlite3.connect(database_path)
    event_count = 0
    quality_count = 0
    event_batch: list[tuple[str, str]] = []
    quality_batch: list[tuple[str, str, int, str, str]] = []
    event_preview: list[dict[str, Any]] = []
    quality_preview: list[dict[str, Any]] = []
    events_materialized = True
    quality_materialized = True
    unmatched: set[str] = set()

    def flush_events() -> None:
        if not event_batch:
            return
        try:
            connection.executemany("INSERT INTO events VALUES (?, ?)", event_batch)
        except sqlite3.IntegrityError as exc:
            raise ValueError("duplicate canonical source_event_id") from exc
        event_batch.clear()

    def flush_quality() -> None:
        if not quality_batch:
            return
        connection.executemany(
            "INSERT INTO quality VALUES (?, ?, ?, ?, ?)", quality_batch
        )
        quality_batch.clear()

    def store_event(event: dict[str, Any]) -> None:
        nonlocal event_count, events_materialized
        source_id = event.get("source_event_id")
        if not isinstance(source_id, str):
            raise ValueError("canonical event source identity is invalid")
        payload = _canonical_json_bytes(event).decode("utf-8").rstrip("\n")
        event_batch.append((source_id, payload))
        event_count += 1
        if events_materialized:
            if event_count <= _SNAPSHOT_RESULT_MATERIALIZATION_LIMIT:
                event_preview.append(event)
            else:
                events_materialized = False
                event_preview.clear()
        if event.get("brick_entity_id") is None:
            unmatched.add(str(event.get("meter_id")))
        if len(event_batch) >= 1000:
            flush_events()

    def store_quality(row: dict[str, Any]) -> None:
        nonlocal quality_count, quality_materialized
        payload = _canonical_json_bytes(row).decode("utf-8").rstrip("\n")
        quality_batch.append(
            (
                str(row["source_relative_path"]),
                str(row["workbook_sheet"]),
                int(row["source_row_number"]),
                str(row["category"]),
                payload,
            )
        )
        quality_count += 1
        if quality_materialized:
            if quality_count <= _SNAPSHOT_RESULT_MATERIALIZATION_LIMIT:
                quality_preview.append(row)
            else:
                quality_materialized = False
                quality_preview.clear()
        if len(quality_batch) >= 1000:
            flush_quality()

    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(
            "CREATE TABLE events (source_event_id TEXT PRIMARY KEY, payload TEXT) "
            "WITHOUT ROWID"
        )
        connection.execute(
            "CREATE TABLE quality (source_relative_path TEXT, workbook_sheet TEXT, "
            "source_row_number INTEGER, category TEXT, payload TEXT)"
        )
        _, _, brick = _canonicalize(
            manifest,
            rules,
            event_sink=store_event,
            quality_sink=store_quality,
        )
        flush_events()
        flush_quality()
        connection.commit()
        ndjson_path = stage / "canonical.ndjson"
        parquet_path = stage / "canonical.parquet"
        quality_path = stage / "source-quality.ndjson"
        _write_payload_query(
            connection,
            "SELECT payload FROM events ORDER BY source_event_id",
            ndjson_path,
        )
        _write_parquet_from_database(connection, parquet_path, event_count)
        _write_payload_query(
            connection,
            "SELECT payload FROM quality ORDER BY source_relative_path, "
            "workbook_sheet, source_row_number, category",
            quality_path,
        )

        source_projection = _manifest_projection(manifest)
        snapshot_manifest: dict[str, Any] = {
            "schema_version": "snapshot-manifest-v1",
            "status": "complete",
            "dataset_version": manifest.dataset_version,
            "source_variant": manifest.source_variant,
            "dataset_manifest_sha256": hashlib.sha256(
                _canonical_json_bytes(source_projection)
            ).hexdigest(),
            "source_files": source_projection["files"],
            "source_locator_fields": [
                "dataset_version",
                "source_variant",
                "source_relative_path",
                "workbook_sheet",
                "source_row_number",
            ],
            "timezone": {
                "name": rules.source_timezone,
                "evidence": rules.timezone_evidence,
            },
            "source_layout": {
                "sha256": rules.source_layout.sha256(),
                "contract": rules.source_layout.to_dict(),
            },
            "measurement_registry": {
                meter_id: asdict(rule)
                for meter_id, rule in sorted(rules.measurements.items())
            },
            "brick": {
                "sha256": brick.sha256,
                "detected_namespace": brick.detected_brick_namespace,
                "matched_meter_count": brick.report.matched_meter_count,
                "brick_entity_count": brick.report.brick_entity_count,
                "unmatched_meter_ids": sorted(unmatched),
            },
            "artifacts": {
                "canonical_ndjson": {
                    "file": "canonical.ndjson",
                    "record_count": event_count,
                    "byte_size": ndjson_path.stat().st_size,
                    "sha256": _sha256(ndjson_path),
                },
                "canonical_parquet": {
                    "file": "canonical.parquet",
                    "record_count": event_count,
                    "byte_size": parquet_path.stat().st_size,
                    "sha256": _sha256(parquet_path),
                },
                "source_quality": {
                    "file": "source-quality.ndjson",
                    "record_count": quality_count,
                    "byte_size": quality_path.stat().st_size,
                    "sha256": _sha256(quality_path),
                },
            },
        }
        (stage / "snapshot-manifest.json").write_bytes(
            _canonical_json_bytes(snapshot_manifest, pretty=True)
        )
        if events_materialized:
            event_preview.sort(key=lambda event: event["source_event_id"])
        if quality_materialized:
            quality_preview.sort(
                key=lambda row: (
                    row["source_relative_path"],
                    row["workbook_sheet"],
                    row["source_row_number"],
                    row["category"],
                )
            )
        connection.close()
        database_path.unlink()
        if output_dir.exists():
            raise FileExistsError(f"immutable snapshot already exists: {output_dir}")
        stage.replace(output_dir)
    except Exception:
        connection.close()
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return SnapshotResult(
        tuple(event_preview) if events_materialized else None,
        tuple(quality_preview) if quality_materialized else None,
        snapshot_manifest,
    )


def write_snapshot(
    events: list[dict[str, Any]],
    ndjson: Path,
    parquet: Path | None = None,
    manifest: DatasetManifest | None = None,
) -> dict[str, Any]:
    """Legacy artifact writer retained for callers that already hold events."""

    ndjson.parent.mkdir(parents=True, exist_ok=True)
    _write_ndjson(events, ndjson)
    result: dict[str, Any] = {
        "event_count": len(events),
        "ndjson_sha256": _sha256(ndjson),
    }
    if parquet is not None:
        parquet.parent.mkdir(parents=True, exist_ok=True)
        _write_parquet(events, parquet)
        result["parquet_sha256"] = _sha256(parquet)
    if manifest is not None:
        result["dataset_version"] = manifest.dataset_version
        result["source_variant"] = manifest.source_variant
    return result

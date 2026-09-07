"""Measured experiment summaries and explicitly hypothetical decision impact."""

from __future__ import annotations

import json
import os
import sqlite3
import statistics
import tempfile
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class IntervalDerivation:
    intervals: tuple[dict[str, Any], ...]
    flagged_records: tuple[dict[str, Any], ...]


def summarize(values: Iterable[float]) -> dict[str, float]:
    items = list(values)
    if not items:
        raise ValueError("cannot summarize empty measurements")
    return {
        "median": statistics.median(items),
        "minimum": min(items),
        "maximum": max(items),
    }


def load_experiment_matrix(path: Path) -> dict[str, Any]:
    """Load and validate the fixed coursework experiment matrix."""

    with path.open("rb") as stream:
        matrix = tomllib.load(stream)
    expected: dict[str, Any] = {
        "correctness": {
            "faults": ["clean", "duplicate", "delayed", "malformed", "combined"],
            "repetitions": 1,
        },
        "scale": {
            "requested_sizes": [100_000, 1_000_000, 5_000_000],
            "final_size_rule": "min(5000000, full_dataset)",
            "repetitions": 3,
            "fault": "clean",
        },
        "recovery": {
            "modes": ["uninterrupted", "interrupted_resumed"],
            "fixture_only": True,
        },
        "watermark": {"retaining": "24 hours", "restrictive": "1 minute"},
        "storage": {
            "formats": [
                "ndjson",
                "parquet_zstd_unpartitioned",
                "parquet_zstd_partitioned",
            ]
        },
        "queries": {
            "names": ["meter", "building", "time_window", "quality", "exposure"]
        },
        "business": {
            "normalized_rates": [0.5, 1.0, 2.0],
            "currency_label": "hypothetical scenario currency units",
        },
        "execution": {
            "require_machine_metadata": True,
            "require_reconciliation": True,
            "full_runs": "designated-machine-only-after-fixture-acceptance",
            "live_demo_max_events": 100_000,
        },
    }
    if matrix != expected:
        raise ValueError("experiment matrix differs from the fixed coursework policy")
    return matrix


def resolve_scale_sizes(full_event_count: int) -> list[int]:
    """Resolve 100K, 1M, and min(5M, full dataset) without duplicates."""

    if full_event_count < 1_000_000:
        raise ValueError("full dataset must contain at least 1,000,000 events")
    requested = [100_000, 1_000_000, min(5_000_000, full_event_count)]
    return list(dict.fromkeys(requested))


def build_experiment_runs(
    matrix: dict[str, Any], full_event_count: int
) -> list[dict[str, Any]]:
    """Resolve stable run identifiers for the complete fixed matrix."""

    runs: list[dict[str, Any]] = []
    for fault in matrix["correctness"]["faults"]:
        runs.append(
            {
                "run_id": f"correctness-{fault}-r01",
                "experiment": "correctness",
                "fault": fault,
                "repetition": 1,
                "event_count": "fixture",
            }
        )
    for mode in matrix["recovery"]["modes"]:
        runs.append(
            {
                "run_id": f"recovery-{mode}-r01",
                "experiment": "recovery",
                "fault": "clean",
                "mode": mode,
                "repetition": 1,
                "event_count": "fixture",
            }
        )
    for mode in ("retaining", "restrictive"):
        runs.append(
            {
                "run_id": f"watermark-{mode}-r01",
                "experiment": "watermark",
                "fault": "delayed",
                "watermark": matrix["watermark"][mode],
                "repetition": 1,
                "event_count": "fixture",
            }
        )
    for size in resolve_scale_sizes(full_event_count):
        for repetition in range(1, matrix["scale"]["repetitions"] + 1):
            runs.append(
                {
                    "run_id": f"scale-{size}-r{repetition:02d}",
                    "experiment": "scale",
                    "fault": "clean",
                    "repetition": repetition,
                    "event_count": size,
                }
            )
    run_ids = [run["run_id"] for run in runs]
    if len(run_ids) != len(set(run_ids)):
        raise ValueError("experiment matrix produced duplicate run IDs")
    return runs


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _flag(event: dict[str, Any], category: str, detail: str) -> dict[str, Any]:
    return {
        "source_event_id": event.get("source_event_id"),
        "meter_id": event.get("meter_id"),
        "event_time_utc": event.get("event_time_utc"),
        "category": category,
        "detail": detail,
    }


def derive_interval_consumption(
    events: Iterable[dict[str, Any]],
) -> IntervalDerivation:
    """Derive intervals only from verified cumulative-kWh meter series."""

    by_meter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_meter[str(event.get("meter_id", ""))].append(event)
    intervals: list[dict[str, Any]] = []
    flagged: list[dict[str, Any]] = []

    for meter_id in sorted(by_meter):
        readings = by_meter[meter_id]
        contracts = {
            (
                reading.get("measurement_kind"),
                reading.get("unit"),
                reading.get("decimal_scale"),
            )
            for reading in readings
        }
        if (
            len(contracts) != 1
            or next(iter(contracts))[0] != "cumulative_energy"
            or next(iter(contracts))[1] != "kWh"
        ):
            flagged.extend(
                _flag(
                    reading,
                    "unverified_cumulative_kwh_semantics",
                    "interval consumption requires one verified "
                    "cumulative_energy/kWh contract",
                )
                for reading in readings
            )
            continue
        scale = next(iter(contracts))[2]
        if isinstance(scale, bool) or not isinstance(scale, int) or scale < 0:
            flagged.extend(
                _flag(
                    reading,
                    "unverified_cumulative_kwh_semantics",
                    "decimal scale is invalid or inconsistent",
                )
                for reading in readings
            )
            continue

        parsed: list[tuple[datetime, dict[str, Any]]] = []
        invalid_time = False
        for reading in readings:
            raw_time = reading.get("event_time_utc")
            try:
                if not isinstance(raw_time, str):
                    raise ValueError
                timestamp = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    raise ValueError
                scaled = reading.get("scaled_value")
                if isinstance(scaled, bool) or not isinstance(scaled, int):
                    raise TypeError
            except (TypeError, ValueError):
                invalid_time = True
                flagged.append(
                    _flag(
                        reading,
                        "invalid_verified_reading",
                        "timestamp or scaled value is invalid",
                    )
                )
                continue
            parsed.append((timestamp, reading))
        if invalid_time:
            # Do not bridge across a missing or invalid cumulative reading.
            continue
        parsed.sort(key=lambda item: (item[0], str(item[1].get("source_event_id", ""))))
        by_time: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
        for timestamp, reading in parsed:
            by_time[timestamp].append(reading)

        baseline: tuple[datetime, dict[str, Any]] | None = None
        after_ambiguous = False
        for timestamp in sorted(by_time):
            same_time = by_time[timestamp]
            if len(same_time) > 1:
                flagged.extend(
                    _flag(
                        reading,
                        "ambiguous_duplicate_timestamp",
                        "multiple physical source rows share this meter timestamp",
                    )
                    for reading in same_time
                )
                baseline = None
                after_ambiguous = True
                continue
            reading = same_time[0]
            if baseline is None:
                category = (
                    "first_after_ambiguous_timestamp"
                    if after_ambiguous
                    else "first_reading_no_interval"
                )
                flagged.append(
                    _flag(
                        reading,
                        category,
                        "a prior unambiguous reading is required for a valid interval",
                    )
                )
                baseline = (timestamp, reading)
                after_ambiguous = False
                continue
            previous_time, previous = baseline
            delta = int(reading["scaled_value"]) - int(previous["scaled_value"])
            if delta < 0:
                flagged.append(
                    _flag(
                        reading,
                        "negative_delta_or_reset",
                        "negative cumulative delta is not valid consumption",
                    )
                )
                baseline = (timestamp, reading)
                continue
            intervals.append(
                {
                    "meter_id": meter_id,
                    "interval_start_utc": previous["event_time_utc"],
                    "interval_end_utc": reading["event_time_utc"],
                    "start_source_event_id": previous.get("source_event_id"),
                    "end_source_event_id": reading.get("source_event_id"),
                    "delta_scaled": delta,
                    "decimal_scale": scale,
                    "unit": "kWh",
                    "delta_kwh": _decimal_text(Decimal(delta).scaleb(-scale)),
                }
            )
            baseline = (timestamp, reading)

    intervals.sort(key=lambda row: (row["meter_id"], row["interval_end_utc"]))
    flagged.sort(
        key=lambda row: (
            str(row.get("meter_id")),
            str(row.get("event_time_utc")),
            str(row.get("source_event_id")),
            row["category"],
        )
    )
    return IntervalDerivation(tuple(intervals), tuple(flagged))


def build_decision_impact_report(
    candidate_events: Iterable[dict[str, Any]],
    verified_events: Iterable[dict[str, Any]],
    *,
    reconciliation_passed: bool,
    verification_seconds: float,
    candidate_storage_bytes: int,
    verification_storage_bytes: int,
    rates: tuple[Decimal, ...] = (
        Decimal("0.5"),
        Decimal("1.0"),
        Decimal("2.0"),
    ),
) -> dict[str, Any]:
    """Compare operational candidate periods with independently verified Gold."""

    if not reconciliation_passed:
        raise ValueError("decision impact requires passing Spark-Hadoop reconciliation")
    if verification_seconds < 0:
        raise ValueError("verification_seconds must be non-negative")
    if candidate_storage_bytes < 0 or verification_storage_bytes < 0:
        raise ValueError("storage measurements must be non-negative")
    if any(rate < 0 for rate in rates):
        raise ValueError("rates must be non-negative")
    candidate = derive_interval_consumption(candidate_events)
    verified = derive_interval_consumption(verified_events)

    def period_map(rows: tuple[dict[str, Any], ...]) -> dict[tuple[str, str], Decimal]:
        return {
            (row["meter_id"], row["interval_end_utc"]): Decimal(row["delta_kwh"])
            for row in rows
        }

    candidate_periods = period_map(candidate.intervals)
    verified_periods = period_map(verified.intervals)
    affected: list[dict[str, str]] = []
    absolute_discrepancy = Decimal(0)
    for key in sorted(set(candidate_periods) | set(verified_periods)):
        candidate_value = candidate_periods.get(key, Decimal(0))
        verified_value = verified_periods.get(key, Decimal(0))
        difference = abs(candidate_value - verified_value)
        if difference == 0:
            continue
        absolute_discrepancy += difference
        affected.append(
            {
                "meter_id": key[0],
                "period_end_utc": key[1],
                "candidate_kwh": _decimal_text(candidate_value),
                "verified_kwh": _decimal_text(verified_value),
                "absolute_kwh_discrepancy": _decimal_text(difference),
            }
        )

    scenarios = [
        {
            "label": "hypothetical normalized tariff scenario",
            "normalized_rate": format(rate, "f"),
            "rate_unit": "scenario currency units per kWh",
            "gross_exposure": _decimal_text(absolute_discrepancy * rate),
            "residual_exposure_after_verification": "0",
            "monetary_interpretation": "scenario only; not an HKUST financial result",
        }
        for rate in rates
    ]
    ratio = (
        verification_storage_bytes / candidate_storage_bytes
        if candidate_storage_bytes
        else None
    )
    return {
        "schema_version": "decision-impact-v1",
        "status": "verified",
        "affected_meter_period_count": len(affected),
        "affected_meter_periods": affected,
        "absolute_kwh_discrepancy": _decimal_text(absolute_discrepancy),
        "candidate_flagged_record_count": len(candidate.flagged_records),
        "candidate_flagged_records": list(candidate.flagged_records),
        "verified_flagged_record_count": len(verified.flagged_records),
        "verified_flagged_records": list(verified.flagged_records),
        "verification_seconds": verification_seconds,
        "candidate_storage_bytes": candidate_storage_bytes,
        "verification_storage_bytes": verification_storage_bytes,
        "storage_overhead_bytes": verification_storage_bytes,
        "storage_overhead_ratio": ratio,
        "tariff_scenarios": scenarios,
    }


def build_decision_impact_report_from_ndjson(
    candidate_path: Path,
    verified_path: Path,
    *,
    candidate_record_count: int,
    verified_record_count: int,
    scratch_directory: Path,
    reconciliation_passed: bool,
    verification_seconds: float,
    candidate_storage_bytes: int,
    verification_storage_bytes: int,
    rates: tuple[Decimal, ...] = (
        Decimal("0.5"),
        Decimal("1.0"),
        Decimal("2.0"),
    ),
) -> dict[str, Any]:
    """Build exact decision impact using disk-backed sorting and interval tables."""

    if not reconciliation_passed:
        raise ValueError("decision impact requires passing Spark-Hadoop reconciliation")
    if verification_seconds < 0:
        raise ValueError("verification_seconds must be non-negative")
    if candidate_storage_bytes < 0 or verification_storage_bytes < 0:
        raise ValueError("storage measurements must be non-negative")
    if any(rate < 0 for rate in rates):
        raise ValueError("rates must be non-negative")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (candidate_record_count, verified_record_count)
    ):
        raise ValueError("decision-impact artifact counts are invalid")
    scratch_directory.mkdir(parents=True, exist_ok=True)
    descriptor, database_name = tempfile.mkstemp(
        prefix=".decision-impact.", suffix=".sqlite3", dir=scratch_directory
    )
    os.close(descriptor)
    database_path = Path(database_name)
    connection = sqlite3.connect(database_path)
    flagged: dict[str, list[dict[str, Any]]] = {
        "candidate": [],
        "verified": [],
    }
    contracts: dict[tuple[str, str], set[tuple[object, object, object]]] = defaultdict(
        set
    )
    invalid_meters: set[tuple[str, str]] = set()
    try:
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute(
            "CREATE TABLE events ("
            "series TEXT NOT NULL, row_sequence INTEGER NOT NULL, "
            "source_event_id TEXT, meter_id TEXT NOT NULL, "
            "event_time_utc TEXT NOT NULL, scaled_value INTEGER, "
            "decimal_scale INTEGER, measurement_kind TEXT, unit TEXT, "
            "valid_reading INTEGER NOT NULL, "
            "PRIMARY KEY (series, row_sequence)) WITHOUT ROWID"
        )
        connection.execute(
            "CREATE TABLE intervals ("
            "series TEXT NOT NULL, meter_id TEXT NOT NULL, "
            "period_end_utc TEXT NOT NULL, delta_scaled INTEGER NOT NULL, "
            "decimal_scale INTEGER NOT NULL, "
            "PRIMARY KEY (series, meter_id, period_end_utc)) WITHOUT ROWID"
        )

        def ingest(path: Path, series: str, expected_count: int) -> None:
            observed = 0
            batch: list[tuple[Any, ...]] = []
            try:
                stream = path.open("r", encoding="utf-8", newline="")
            except (OSError, UnicodeError) as exc:
                raise ValueError(
                    f"cannot read {series} decision-impact events"
                ) from exc
            with stream:
                for line_number, line in enumerate(stream, start=1):
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"{series} event line {line_number} is invalid JSON"
                        ) from exc
                    if not isinstance(event, dict):
                        raise ValueError(
                            f"{series} event line {line_number} is not an object"
                        )
                    meter_id = str(event.get("meter_id", ""))
                    event_time = event.get("event_time_utc")
                    scaled = event.get("scaled_value")
                    valid = True
                    try:
                        if not isinstance(event_time, str):
                            raise ValueError
                        timestamp = datetime.fromisoformat(
                            event_time.replace("Z", "+00:00")
                        )
                        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                            raise ValueError
                        if isinstance(scaled, bool) or not isinstance(scaled, int):
                            raise TypeError
                    except (TypeError, ValueError):
                        valid = False
                        invalid_meters.add((series, meter_id))
                    contract = (
                        event.get("measurement_kind"),
                        event.get("unit"),
                        event.get("decimal_scale"),
                    )
                    try:
                        contracts[(series, meter_id)].add(contract)
                    except TypeError as exc:
                        raise ValueError(
                            f"{series} event line {line_number} has invalid "
                            "contract fields"
                        ) from exc
                    batch.append(
                        (
                            series,
                            line_number,
                            event.get("source_event_id"),
                            meter_id,
                            event_time if isinstance(event_time, str) else "",
                            scaled
                            if isinstance(scaled, int) and not isinstance(scaled, bool)
                            else None,
                            event.get("decimal_scale"),
                            event.get("measurement_kind"),
                            event.get("unit"),
                            int(valid),
                        )
                    )
                    observed += 1
                    if len(batch) >= 1000:
                        connection.executemany(
                            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            batch,
                        )
                        batch.clear()
                if batch:
                    connection.executemany(
                        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        batch,
                    )
            if observed != expected_count:
                raise ValueError(f"{series} decision-impact record count mismatch")

        ingest(candidate_path, "candidate", candidate_record_count)
        ingest(verified_path, "verified", verified_record_count)
        connection.execute(
            "CREATE INDEX events_series_meter_time ON events "
            "(series, meter_id, event_time_utc, source_event_id, row_sequence)"
        )
        connection.commit()

        def cumulative_scale(series: str, meter_id: str) -> int | None:
            contract_set = contracts[(series, meter_id)]
            if len(contract_set) != 1:
                return None
            kind, unit, scale = next(iter(contract_set))
            if (
                kind != "cumulative_energy"
                or unit != "kWh"
                or isinstance(scale, bool)
                or not isinstance(scale, int)
                or scale < 0
            ):
                return None
            return scale

        def row_event(row: tuple[Any, ...]) -> dict[str, Any]:
            return {
                "source_event_id": row[0],
                "meter_id": row[1],
                "event_time_utc": row[2] or None,
            }

        def derive(series: str) -> None:
            cursor = connection.execute(
                "SELECT source_event_id, meter_id, event_time_utc, scaled_value, "
                "decimal_scale, measurement_kind, unit, valid_reading "
                "FROM events WHERE series = ? "
                "ORDER BY meter_id, event_time_utc, source_event_id, row_sequence",
                (series,),
            )
            current_meter: str | None = None
            current_time: str | None = None
            same_time: list[tuple[Any, ...]] = []
            baseline: tuple[Any, ...] | None = None
            after_ambiguous = False

            def flush_group() -> None:
                nonlocal baseline, after_ambiguous
                if not same_time or current_meter is None:
                    return
                scale = cumulative_scale(series, current_meter)
                if scale is None or (series, current_meter) in invalid_meters:
                    return
                if len(same_time) > 1:
                    flagged[series].extend(
                        _flag(
                            row_event(row),
                            "ambiguous_duplicate_timestamp",
                            "multiple physical source rows share this meter timestamp",
                        )
                        for row in same_time
                    )
                    baseline = None
                    after_ambiguous = True
                    return
                reading = same_time[0]
                if baseline is None:
                    category = (
                        "first_after_ambiguous_timestamp"
                        if after_ambiguous
                        else "first_reading_no_interval"
                    )
                    flagged[series].append(
                        _flag(
                            row_event(reading),
                            category,
                            "a prior unambiguous reading is required for a "
                            "valid interval",
                        )
                    )
                    baseline = reading
                    after_ambiguous = False
                    return
                delta = int(reading[3]) - int(baseline[3])
                if delta < 0:
                    flagged[series].append(
                        _flag(
                            row_event(reading),
                            "negative_delta_or_reset",
                            "negative cumulative delta is not valid consumption",
                        )
                    )
                    baseline = reading
                    return
                connection.execute(
                    "INSERT INTO intervals VALUES (?, ?, ?, ?, ?)",
                    (series, current_meter, reading[2], delta, scale),
                )
                baseline = reading

            for row in cursor:
                meter_id = str(row[1])
                event_time = str(row[2])
                if current_meter is not None and (
                    meter_id != current_meter or event_time != current_time
                ):
                    flush_group()
                    same_time = []
                    if meter_id != current_meter:
                        baseline = None
                        after_ambiguous = False
                current_meter = meter_id
                current_time = event_time
                same_time.append(tuple(row))
                if cumulative_scale(series, meter_id) is None:
                    flagged[series].append(
                        _flag(
                            row_event(tuple(row)),
                            "unverified_cumulative_kwh_semantics",
                            "interval consumption requires one verified "
                            "cumulative_energy/kWh contract",
                        )
                    )
                elif (series, meter_id) in invalid_meters and not bool(row[7]):
                    flagged[series].append(
                        _flag(
                            row_event(tuple(row)),
                            "invalid_verified_reading",
                            "timestamp or scaled value is invalid",
                        )
                    )
            flush_group()
            connection.commit()

        derive("candidate")
        derive("verified")
        affected: list[dict[str, str]] = []
        absolute_discrepancy = Decimal(0)
        comparison = connection.execute(
            "SELECT meter_id, period_end_utc, "
            "MAX(CASE WHEN series = 'candidate' THEN delta_scaled END), "
            "MAX(CASE WHEN series = 'candidate' THEN decimal_scale END), "
            "MAX(CASE WHEN series = 'verified' THEN delta_scaled END), "
            "MAX(CASE WHEN series = 'verified' THEN decimal_scale END) "
            "FROM intervals GROUP BY meter_id, period_end_utc "
            "ORDER BY meter_id, period_end_utc"
        )
        for (
            meter_id,
            period_end,
            candidate_delta,
            candidate_scale,
            verified_delta,
            verified_scale,
        ) in comparison:
            candidate_value = (
                Decimal(0)
                if candidate_delta is None
                else Decimal(int(candidate_delta)).scaleb(-int(candidate_scale))
            )
            verified_value = (
                Decimal(0)
                if verified_delta is None
                else Decimal(int(verified_delta)).scaleb(-int(verified_scale))
            )
            difference = abs(candidate_value - verified_value)
            if difference == 0:
                continue
            absolute_discrepancy += difference
            affected.append(
                {
                    "meter_id": str(meter_id),
                    "period_end_utc": str(period_end),
                    "candidate_kwh": _decimal_text(candidate_value),
                    "verified_kwh": _decimal_text(verified_value),
                    "absolute_kwh_discrepancy": _decimal_text(difference),
                }
            )
        for rows in flagged.values():
            rows.sort(
                key=lambda row: (
                    str(row.get("meter_id")),
                    str(row.get("event_time_utc")),
                    str(row.get("source_event_id")),
                    row["category"],
                )
            )
        scenarios = [
            {
                "label": "hypothetical normalized tariff scenario",
                "normalized_rate": format(rate, "f"),
                "rate_unit": "scenario currency units per kWh",
                "gross_exposure": _decimal_text(absolute_discrepancy * rate),
                "residual_exposure_after_verification": "0",
                "monetary_interpretation": (
                    "scenario only; not an HKUST financial result"
                ),
            }
            for rate in rates
        ]
        ratio = (
            verification_storage_bytes / candidate_storage_bytes
            if candidate_storage_bytes
            else None
        )
        return {
            "schema_version": "decision-impact-v1",
            "status": "verified",
            "affected_meter_period_count": len(affected),
            "affected_meter_periods": affected,
            "absolute_kwh_discrepancy": _decimal_text(absolute_discrepancy),
            "candidate_flagged_record_count": len(flagged["candidate"]),
            "candidate_flagged_records": flagged["candidate"],
            "verified_flagged_record_count": len(flagged["verified"]),
            "verified_flagged_records": flagged["verified"],
            "verification_seconds": verification_seconds,
            "candidate_storage_bytes": candidate_storage_bytes,
            "verification_storage_bytes": verification_storage_bytes,
            "storage_overhead_bytes": verification_storage_bytes,
            "storage_overhead_ratio": ratio,
            "tariff_scenarios": scenarios,
        }
    finally:
        connection.close()
        try:
            database_path.unlink()
        except FileNotFoundError:
            pass


def decision_impact(
    candidate_kwh: float,
    verified_kwh: float,
    rates: tuple[float, ...] = (0.5, 1.0, 2.0),
) -> list[dict[str, float]]:
    """Compatibility helper for scalar examples; values are scenarios only."""

    if any(rate < 0 for rate in rates):
        raise ValueError("rates must be non-negative")
    discrepancy = abs(candidate_kwh - verified_kwh)
    return [
        {
            "rate": rate,
            "candidate_exposure": candidate_kwh * rate,
            "verified_exposure": verified_kwh * rate,
            "absolute_discrepancy": discrepancy * rate,
        }
        for rate in rates
    ]

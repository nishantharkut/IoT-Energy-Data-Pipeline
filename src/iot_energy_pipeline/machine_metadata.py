"""Create and validate explicit experiment-machine metadata."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import platform
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

MACHINE_FACT_FIELDS = {
    "hostname",
    "operating_system",
    "processor",
    "logical_cpu_count",
    "memory_bytes",
}
SOFTWARE_FIELDS = {"python", "docker", "kafka", "spark", "hadoop"}


def validate_machine_metadata(value: Mapping[str, Any]) -> None:
    """Validate the closed machine-metadata-v1 semantic contract."""

    required = {
        "schema_version",
        "captured_at_utc",
        "execution_context",
        *MACHINE_FACT_FIELDS,
        "software",
    }
    if set(value) != required or value.get("schema_version") != "machine-metadata-v1":
        raise ValueError("machine metadata is incomplete or contains unknown fields")
    for field in (
        "captured_at_utc",
        "execution_context",
        "hostname",
        "operating_system",
        "processor",
    ):
        if not isinstance(value.get(field), str) or not value[field]:
            raise ValueError("machine metadata is incomplete")
    if value["execution_context"] not in {"fixture", "designated_machine"}:
        raise ValueError("machine metadata execution context is invalid")
    try:
        timestamp = datetime.fromisoformat(
            str(value["captured_at_utc"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("machine metadata capture timestamp is invalid") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() != timezone.utc.utcoffset(
        None
    ):
        raise ValueError("machine metadata capture timestamp must be UTC")
    for field in ("logical_cpu_count", "memory_bytes"):
        number = value.get(field)
        if isinstance(number, bool) or not isinstance(number, int) or number < 1:
            raise ValueError("machine metadata is incomplete")
    software = value.get("software")
    if (
        not isinstance(software, dict)
        or set(software) != SOFTWARE_FIELDS
        or any(not isinstance(item, str) or not item for item in software.values())
    ):
        raise ValueError("machine metadata software inventory is incomplete")


def _total_memory_bytes() -> int:
    if os.name == "nt":

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", ctypes.c_ulong),
                ("memory_load", ctypes.c_ulong),
                ("total_physical", ctypes.c_ulonglong),
                ("available_physical", ctypes.c_ulonglong),
                ("total_page_file", ctypes.c_ulonglong),
                ("available_page_file", ctypes.c_ulonglong),
                ("total_virtual", ctypes.c_ulonglong),
                ("available_virtual", ctypes.c_ulonglong),
                ("available_extended_virtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(MemoryStatus)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError("GlobalMemoryStatusEx failed")
        return int(status.total_physical)
    sysconf = getattr(os, "sysconf", None)
    if not callable(sysconf):
        raise RuntimeError("physical memory size is unavailable")
    page_size = sysconf("SC_PAGE_SIZE")
    physical_pages = sysconf("SC_PHYS_PAGES")
    return int(page_size * physical_pages)


def detect_machine_facts() -> dict[str, Any]:
    """Read stable local host facts without invoking external programs."""

    cpu_count = os.cpu_count()
    if cpu_count is None or cpu_count < 1:
        raise RuntimeError("logical CPU count is unavailable")
    processor = platform.processor().strip() or platform.machine().strip()
    if not processor:
        raise RuntimeError("processor description is unavailable")
    return {
        "hostname": socket.gethostname(),
        "operating_system": platform.platform(),
        "processor": processor,
        "logical_cpu_count": cpu_count,
        "memory_bytes": _total_memory_bytes(),
    }


def write_machine_metadata(
    *,
    output: Path,
    execution_context: str,
    software: Mapping[str, str],
    captured_at_utc: str | None = None,
    machine_facts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Write one immutable metadata document; do not probe distributed services."""

    if output.exists():
        raise FileExistsError(f"immutable machine metadata already exists: {output}")
    facts = dict(detect_machine_facts() if machine_facts is None else machine_facts)
    if set(facts) != MACHINE_FACT_FIELDS:
        raise ValueError("machine facts are incomplete or contain unknown fields")
    captured = captured_at_utc or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    value: dict[str, Any] = {
        "schema_version": "machine-metadata-v1",
        "captured_at_utc": captured,
        "execution_context": execution_context,
        **facts,
        "software": dict(software),
    }
    validate_machine_metadata(value)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    with output.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture machine facts; software versions are explicit inputs"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--execution-context",
        choices=("fixture", "designated_machine"),
        required=True,
    )
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--docker-version", required=True)
    parser.add_argument("--kafka-version", required=True)
    parser.add_argument("--spark-version", required=True)
    parser.add_argument("--hadoop-version", required=True)
    args = parser.parse_args()
    result = write_machine_metadata(
        output=args.output,
        execution_context=args.execution_context,
        software={
            "python": args.python_version,
            "docker": args.docker_version,
            "kafka": args.kafka_version,
            "spark": args.spark_version,
            "hadoop": args.hadoop_version,
        },
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

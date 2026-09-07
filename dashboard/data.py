"""Read-only validation and loading of sealed finalized run directories."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, cast

REQUIRED_VIEWS = (
    "provenance_status",
    "throughput_layers",
    "fault_disposition",
    "energy_quality",
    "storage_queries",
    "reconciliation",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_path(run_dir: Path, relative: object) -> Path:
    if not isinstance(relative, str) or not relative or "\\" in relative:
        raise ValueError("unsafe artifact path")
    posix = PurePosixPath(relative)
    if posix.is_absolute() or any(part in {"", ".", ".."} for part in posix.parts):
        raise ValueError("unsafe artifact path")
    target = run_dir.joinpath(*posix.parts)
    try:
        target.resolve(strict=False).relative_to(run_dir.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise ValueError("unsafe artifact path") from exc
    return target


def load_finalized_run(run_dir: Path) -> dict[str, Any]:
    """Load only a complete, reconciled, sealed and hash-verified run."""

    manifest_path = run_dir / "run-manifest.json"
    seal_path = run_dir / "run-manifest.sha256"
    if (
        not manifest_path.is_file()
        or manifest_path.is_symlink()
        or not seal_path.is_file()
        or seal_path.is_symlink()
    ):
        raise ValueError("run manifest or integrity seal is missing")
    expected_manifest_hash = seal_path.read_text(encoding="ascii").strip()
    if (
        len(expected_manifest_hash) != 64
        or any(
            character not in "0123456789abcdef" for character in expected_manifest_hash
        )
        or _sha256(manifest_path) != expected_manifest_hash
    ):
        raise ValueError("run manifest integrity check failed")
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("run manifest is not valid UTF-8 JSON") from exc
    if not isinstance(data, dict) or data.get("schema_version") != "finalized-run-v1":
        raise ValueError("unsupported finalized run schema")
    if data.get("status") != "finalized":
        raise ValueError("run is not finalized")
    if data.get("reconciled") is not True:
        raise ValueError("run has not passed reconciliation")
    if data.get("immutable") is not True:
        raise ValueError("run is not declared immutable")

    artifacts = data.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("finalized run has no artifact registry")
    expected_files: set[str] = set()
    for name, descriptor in artifacts.items():
        if not isinstance(descriptor, dict):
            raise ValueError(f"artifact integrity descriptor is invalid: {name}")
        target = _artifact_path(run_dir, descriptor.get("relative_path"))
        relative = target.relative_to(run_dir).as_posix()
        if relative in expected_files:
            raise ValueError("artifact registry contains duplicate paths")
        expected_files.add(relative)
        if not target.is_file() or target.is_symlink():
            raise ValueError(f"artifact integrity check failed: {name}")
        if target.stat().st_size != descriptor.get("byte_size"):
            raise ValueError(f"artifact integrity check failed: {name}")
        if _sha256(target) != descriptor.get("sha256"):
            raise ValueError(f"artifact integrity check failed: {name}")

    actual_files: set[str] = set()
    for target in run_dir.rglob("*"):
        if target.is_symlink():
            raise ValueError("artifact integrity check failed: symbolic link")
        if not target.is_file():
            continue
        relative = target.relative_to(run_dir).as_posix()
        if relative not in {"run-manifest.json", "run-manifest.sha256"}:
            actual_files.add(relative)
    if actual_files != expected_files:
        raise ValueError("artifact registry does not match sealed run files")

    views = data.get("views")
    if not isinstance(views, dict) or set(views) != set(REQUIRED_VIEWS):
        raise ValueError("required dashboard views are missing or unexpected")
    return cast(dict[str, Any], data)

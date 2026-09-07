"""Streaming verification for the downloaded Dryad package wrapper."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import IO
from zipfile import BadZipFile, ZipFile


@dataclass(frozen=True)
class DryadPackageContract:
    """Primary-source integrity values for one Dryad release."""

    dataset_doi: str
    dataset_version: str
    inner_archive_size: int
    inner_archive_sha256: str
    readme_size: int

    def __post_init__(self) -> None:
        if not self.dataset_doi.strip() or not self.dataset_version.strip():
            raise ValueError("Dryad DOI and version must be non-empty")
        if self.inner_archive_size < 1 or self.readme_size < 1:
            raise ValueError("Dryad file sizes must be positive")
        digest = self.inner_archive_sha256
        if len(digest) != 64 or any(
            value not in "0123456789abcdef" for value in digest
        ):
            raise ValueError("inner archive SHA-256 must be lowercase hexadecimal")


HKUST_DRYAD_V5 = DryadPackageContract(
    dataset_doi="10.5061/dryad.k3j9kd5h6",
    dataset_version="v5-2024-08-01",
    inner_archive_size=1_432_480_964,
    inner_archive_sha256=(
        "f2446158573311bda2b1fbd8a114bb1479ac8d4a0e11c395ca3b920676eb0c8a"
    ),
    readme_size=4_104,
)


def _stream_sha256(stream: IO[bytes]) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return _stream_sha256(stream)


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def _write_atomic_new(path: Path, payload: bytes) -> None:
    if path.exists():
        raise FileExistsError(f"immutable package report already exists: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
        if path.exists():
            raise FileExistsError(
                f"immutable package report already exists: {path.name}"
            )
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def verify_dryad_package(
    package: Path,
    output: Path,
    *,
    contract: DryadPackageContract = HKUST_DRYAD_V5,
) -> dict[str, object]:
    """Verify a two-file DOI ZIP without extracting its inner data archive."""

    if not package.is_file() or package.is_symlink():
        raise ValueError("Dryad package must be a regular non-symlink file")
    if output.exists():
        raise FileExistsError(f"immutable package report already exists: {output.name}")

    expected_names = ("All_Data.zip", "README.md")
    try:
        with ZipFile(package) as archive:
            infos = archive.infolist()
            names = Counter(info.filename for info in infos)
            if len(infos) != len(expected_names) or names != Counter(expected_names):
                raise ValueError(
                    "Dryad package must contain exactly All_Data.zip and README.md"
                )
            if any(info.is_dir() or info.flag_bits & 0x1 for info in infos):
                raise ValueError(
                    "Dryad package entries must be unencrypted regular files"
                )
            by_name = {info.filename: info for info in infos}
            expected_sizes = {
                "All_Data.zip": contract.inner_archive_size,
                "README.md": contract.readme_size,
            }
            files: dict[str, dict[str, object]] = {}
            for name in expected_names:
                info = by_name[name]
                if info.file_size != expected_sizes[name]:
                    raise ValueError(f"{name} integrity mismatch: unexpected byte size")
                with archive.open(info, "r") as stream:
                    digest = _stream_sha256(stream)
                files[name] = {"byte_size": info.file_size, "sha256": digest}
    except (BadZipFile, OSError, RuntimeError) as exc:
        raise ValueError(f"invalid Dryad package: {package.name}") from exc

    if files["All_Data.zip"]["sha256"] != contract.inner_archive_sha256:
        raise ValueError("All_Data.zip integrity mismatch: SHA-256 differs")

    report: dict[str, object] = {
        "schema_version": "dryad-package-verification-v1",
        "status": "verified",
        "dataset_doi": contract.dataset_doi,
        "dataset_version": contract.dataset_version,
        "container": {
            "file_name": package.name,
            "byte_size": package.stat().st_size,
            "sha256": _file_sha256(package),
        },
        "files": files,
    }
    _write_atomic_new(output, _json_bytes(report))
    return report

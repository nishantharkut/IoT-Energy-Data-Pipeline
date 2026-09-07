"""Dataset manifest registration and integrity verification."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Literal

SourceVariant = Literal["raw", "clean"]


@dataclass(frozen=True)
class FileManifest:
    """Manifest entry for a single registered file."""

    relative_path: str
    byte_size: int
    sha256: str
    media_kind: Literal["workbook", "brick_ttl"]


@dataclass(frozen=True)
class DatasetManifest:
    """Manifest for a registered dataset variant."""

    dataset_version: str
    source_variant: SourceVariant
    files: tuple[FileManifest, ...] = field(default_factory=tuple)
    _root: Path | None = field(default=None, compare=False, repr=False)

    def to_json(self, path: Path) -> None:
        """Write manifest to JSON file atomically."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "dataset_version": self.dataset_version,
            "source_variant": self.source_variant,
            "files": [
                {
                    "relative_path": f.relative_path,
                    "byte_size": f.byte_size,
                    "sha256": f.sha256,
                    "media_kind": f.media_kind,
                }
                for f in self.files
            ],
        }
        content = json.dumps(data, sort_keys=True, indent=2, ensure_ascii=True)
        content += "\n"

        fd, tmp_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=path.name + ".",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
                f.write(content)
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @classmethod
    def from_json(cls, path: Path, root: Path | None = None) -> "DatasetManifest":
        """Load manifest from JSON file.

        If root is provided, validates that each registered file exists,
        is a safe relative path, is a regular non-symlink file,
        and matches the recorded size and hash.
        """
        content = path.read_text(encoding="utf-8")
        data = json.loads(content)

        files = tuple(
            FileManifest(
                relative_path=f["relative_path"],
                byte_size=f["byte_size"],
                sha256=f["sha256"],
                media_kind=f["media_kind"],
            )
            for f in data["files"]
        )

        manifest = cls(
            dataset_version=data["dataset_version"],
            source_variant=data["source_variant"],
            files=files,
            _root=root,
        )

        if root is not None:
            manifest._validate_files(root)

        return manifest

    def _validate_files(self, root: Path) -> None:
        """Validate that all files are safe paths and match size/hash."""
        for fm in self.files:
            rel_path = fm.relative_path

            # Check for empty path
            if not rel_path:
                raise ValueError("Registered file has empty relative path")

            # Check for dot or dot-dot components in path string
            # Use string matching since PurePosixPath normalizes them away
            if "/./" in rel_path or rel_path.startswith("./"):
                raise ValueError(
                    f"Registered file has dot component in path: {rel_path}"
                )
            if (
                "/../" in rel_path
                or rel_path.startswith("../")
                or rel_path.endswith("/..")
            ):
                raise ValueError(
                    f"Registered file has dot-dot component in path: {rel_path}"
                )
            # Check for trailing or standalone dot/dotdot
            parts = rel_path.split("/")
            for part in parts:
                if part == ".":
                    raise ValueError(
                        f"Registered file has dot component in path: {rel_path}"
                    )
                if part == "..":
                    raise ValueError(
                        f"Registered file has dot-dot component in path: {rel_path}"
                    )

            # Use PurePosixPath for additional validation
            posix_path = PurePosixPath(rel_path)

            # Check for absolute path
            if posix_path.is_absolute():
                raise ValueError(f"Registered file has absolute path: {rel_path}")

            # Resolve the file path
            file_path = root / rel_path

            # Check file exists
            if not file_path.exists():
                raise ValueError(f"Registered file missing: {rel_path}")

            # Check it's a regular file, not a symlink
            if file_path.is_symlink():
                raise ValueError(f"Registered file is a symlink: {rel_path}")

            if not file_path.is_file():
                raise ValueError(f"Registered path is not a regular file: {rel_path}")

            actual_size = file_path.stat().st_size
            if actual_size != fm.byte_size:
                raise ValueError(
                    f"File size changed for {rel_path}: "
                    f"expected {fm.byte_size}, got {actual_size}"
                )
            actual_hash = _compute_sha256(file_path)
            if actual_hash != fm.sha256:
                raise ValueError(f"File hash changed for {rel_path}")


def _compute_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a file using streaming."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_media_kind(path: Path) -> Literal["workbook", "brick_ttl"]:
    """Determine media kind from file extension."""
    ext = path.suffix.lower()
    if ext == ".xlsx":
        return "workbook"
    elif ext == ".ttl":
        return "brick_ttl"
    raise ValueError(f"Unsupported file type: {path}")


def _tokenize_path(path: Path) -> list[str]:
    """Extract all tokens from a path (directory names and file stem).

    Tokenizes on non-alphanumeric boundaries.
    """
    tokens: list[str] = []

    for part in path.parts[:-1]:
        for token in re.split(r"[^a-zA-Z0-9]+", part):
            if token:
                tokens.append(token.lower())

    stem = path.stem
    for token in re.split(r"[^a-zA-Z0-9]+", stem):
        if token:
            tokens.append(token.lower())

    return tokens


def _check_raw_clean_conflict(
    tokens: list[str],
    source_variant: SourceVariant,
) -> str | None:
    """Check for raw/clean variant conflicts.

    Returns error message if conflict found, None otherwise.
    """
    for token in tokens:
        if source_variant == "raw":
            if token == "clean" or token == "cleaned":
                return f"Raw source variant rejects path with '{token}'"
        elif source_variant == "clean":
            if token == "raw":
                return "Clean source variant rejects path with 'raw'"

    return None


def _collect_all_files(root: Path) -> list[Path]:
    """Recursively collect all eligible files from root.

    Collects files first, then sorts globally by POSIX relative path.
    Rejects file symlinks. Does not follow directory symlinks.
    """
    files: list[Path] = []

    def scan_directory(directory: Path) -> None:
        for item in directory.iterdir():
            # Skip directory symlinks (do not follow)
            if item.is_symlink() and item.is_dir():
                continue

            if item.is_dir():
                # Recurse into real directories only
                scan_directory(item)
            elif item.is_file():
                # Check if file is a symlink - reject it
                if item.is_symlink():
                    raise ValueError(f"File is a symlink: {item}")

                ext = item.suffix.lower()
                if ext in (".xlsx", ".ttl"):
                    files.append(item)

    scan_directory(root)

    # Sort globally by POSIX relative path
    files.sort(key=lambda p: p.relative_to(root).as_posix())

    return files


def register_dataset(
    root: Path,
    output: Path,
    *,
    dataset_version: str,
    source_variant: SourceVariant,
) -> DatasetManifest:
    """Register a dataset root directory.

    Args:
        root: Root directory containing source files.
        output: Path to write manifest JSON.
        dataset_version: Dataset version identifier (non-empty).
        source_variant: Either "raw" or "clean".

    Returns:
        DatasetManifest with registered files.

    Raises:
        ValueError: If validation fails.
    """
    # Validate root exists and is a directory
    if not root.exists():
        raise ValueError(f"Root path does not exist: {root}")
    if root.is_symlink():
        raise ValueError(f"Root path is a symlink: {root}")
    if not root.is_dir():
        raise ValueError(f"Root path is not a directory: {root}")

    # Validate dataset_version
    stripped_version = dataset_version.strip()
    if not stripped_version:
        raise ValueError("dataset_version must be non-empty")

    # Validate source_variant
    if source_variant not in ("raw", "clean"):
        raise ValueError(
            f"source_variant must be 'raw' or 'clean', got: {source_variant!r}"
        )

    # Collect all files first, then sort globally
    file_paths = _collect_all_files(root)

    # Check for empty registration
    if not file_paths:
        raise ValueError(f"No files to register in {root}")

    # Register files
    file_manifests: list[FileManifest] = []

    for file_path in file_paths:
        rel_path = file_path.relative_to(root)
        rel_path_str = rel_path.as_posix()

        # Check for raw/clean conflicts
        tokens = _tokenize_path(rel_path)
        error = _check_raw_clean_conflict(tokens, source_variant)
        if error:
            raise ValueError(f"{error} in {rel_path_str}")

        # Compute size and hash
        byte_size = file_path.stat().st_size
        sha256 = _compute_sha256(file_path)
        media_kind = _get_media_kind(file_path)

        file_manifests.append(
            FileManifest(
                relative_path=rel_path_str,
                byte_size=byte_size,
                sha256=sha256,
                media_kind=media_kind,
            )
        )

    # Create manifest
    manifest = DatasetManifest(
        dataset_version=stripped_version,
        source_variant=source_variant,
        files=tuple(file_manifests),
        _root=root,
    )

    # Write manifest
    manifest.to_json(output)

    return manifest

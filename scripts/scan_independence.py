"""Repository independence scanner."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass

DRIVE_LETTER_PATH = re.compile(r"[A-Za-z]:(?:\\|/)[^\"'\s]*")

BINARY_SUFFIXES = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".bmp",
        ".ico",
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".zip",
        ".tar",
        ".gz",
        ".rar",
        ".7z",
        ".war",
        ".jar",
        ".class",
        ".exe",
        ".dll",
        ".so",
        ".dylib",
        ".mp3",
        ".mp4",
        ".avi",
        ".mov",
        ".mkv",
        ".wav",
        ".flac",
        ".aac",
        ".ogg",
        ".sqlite",
        ".db",
        ".mdb",
        ".accdb",
        ".parquet",
        ".ndjson",
    }
)

EXCLUDED_SUFFIXES = frozenset(
    {
        "configs/forbidden-terms.txt",
        "tests/contract/test_repository_independence.py",
    }
)

SCOPE_PREFIXES = (
    "src/",
    "scripts/",
    "jobs/",
    "dashboard/",
    "queries/",
    "schemas/",
    "reports/",
    "docs/",
)


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    rule: str
    matched: str


def _normalize(p: str) -> str:
    return p.replace("\\", "/").lower()


def _has_excluded_suffix(p: str) -> bool:
    n = _normalize(p)
    for suffix in EXCLUDED_SUFFIXES:
        if n.endswith(suffix):
            return True
    parts = n.split("/")
    for i in range(len(parts) - 1):
        if parts[i] == "docs" and parts[i + 1] == "superpowers":
            return True
    if ".git" in parts:
        return True
    return pathlib.Path(p).suffix.lower() in BINARY_SUFFIXES


def _is_scope(rel: str) -> bool:
    n = _normalize(rel)
    if n in ("readme.md", "agents.md"):
        return True
    for prefix in SCOPE_PREFIXES:
        if n.startswith(prefix):
            return not n.startswith("docs/superpowers")
    return False


def _validate_tracked(path: str) -> None:
    if not path:
        raise RuntimeError("Empty tracked path")
    is_abs = path.startswith("/") or pathlib.Path(path).is_absolute()
    if is_abs or re.match(r"^[A-Za-z]:", path):
        raise RuntimeError(f"Absolute tracked path: {path}")
    if ".." in pathlib.Path(path).parts:
        raise RuntimeError(f"Parent traversal in tracked path: {path}")


def load_forbidden_names(config_path: str) -> list[str]:
    p = pathlib.Path(config_path)
    if not p.is_file():
        raise RuntimeError(f"Config not found: {config_path}")
    try:
        content = p.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        raise RuntimeError(f"Config not UTF-8: {config_path}") from e
    except OSError as e:
        raise RuntimeError(f"Cannot read config: {config_path}") from e
    return [
        ln.strip().lower()
        for ln in content.splitlines()
        if ln.strip() and not ln.startswith("#")
    ]


def get_tracked_files() -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            capture_output=True,
            check=True,
            timeout=30,
        )
        files = [f for f in result.stdout.rstrip(b"\0").split(b"\0") if f]
        if not files:
            raise RuntimeError("git ls-files returned no files")
        try:
            return [f.decode("utf-8") for f in files]
        except UnicodeDecodeError as e:
            raise RuntimeError("git ls-files output contains invalid UTF-8") from e
    except subprocess.CalledProcessError as e:
        stderr = (
            e.stderr
            if isinstance(e.stderr, str)
            else (e.stderr.decode("utf-8", errors="replace") if e.stderr else "")
        )
        raise RuntimeError(f"git ls-files failed: {stderr}") from e
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("git ls-files timed out") from e
    except FileNotFoundError as e:
        raise RuntimeError("git not found") from e


def scan_text(content: str, filename: str, forbidden: list[str]) -> list[Violation]:
    violations = []
    for i, line in enumerate(content.splitlines(), 1):
        for m in DRIVE_LETTER_PATH.finditer(line):
            violations.append(
                Violation(filename, i, "drive_letter_absolute_path", m.group())
            )
        lower = line.lower()
        for name in forbidden:
            if name in lower:
                violations.append(Violation(filename, i, "forbidden_name", name))
    return violations


def scan_paths(paths: list[str], forbidden: list[str]) -> list[Violation]:
    violations = []
    for path in paths:
        if _has_excluded_suffix(path):
            continue
        p = pathlib.Path(path)
        if not p.exists():
            raise RuntimeError(f"File not found: {path}")
        try:
            content = p.read_text(encoding="utf-8")
        except UnicodeDecodeError as e:
            raise RuntimeError(f"Not UTF-8: {path}") from e
        except OSError as e:
            raise RuntimeError(f"Cannot read: {path}") from e
        violations.extend(scan_text(content, path, forbidden))
    return violations


def scan_repository(
    forbidden: list[str] | None = None,
    tracked_file_provider: Callable[[], Sequence[str]] | None = None,
    repository_root: pathlib.Path | None = None,
) -> list[Violation]:
    if forbidden is None:
        forbidden = []
    if repository_root is None:
        repository_root = pathlib.Path.cwd()

    if tracked_file_provider:
        try:
            tracked = list(tracked_file_provider())
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Provider failed: {e}") from e
    else:
        tracked = get_tracked_files()

    for rel in tracked:
        _validate_tracked(rel)

    scoped = [r for r in tracked if _is_scope(r) and not _has_excluded_suffix(r)]
    violations = []
    for rel in scoped:
        p = repository_root / rel
        if not p.exists():
            raise RuntimeError(f"Tracked file not found: {rel}")
        try:
            content = p.read_text(encoding="utf-8")
        except UnicodeDecodeError as e:
            raise RuntimeError(f"Not UTF-8: {rel}") from e
        except OSError as e:
            raise RuntimeError(f"Cannot read: {rel}") from e
        violations.extend(scan_text(content, rel, forbidden))
    return violations


def main() -> int:
    try:
        root = pathlib.Path(__file__).parent.parent
        config = root / "configs" / "forbidden-terms.txt"
        forbidden = load_forbidden_names(str(config))
        violations = scan_repository(forbidden=forbidden)
        if violations:
            print("Violations found:")
            for v in violations:
                print(f"  {v.path}:{v.line}: [{v.rule}] {v.matched}")
            return 1
        print("Check passed.")
        return 0
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

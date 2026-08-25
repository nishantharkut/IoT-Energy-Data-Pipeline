"""Contract tests for repository independence scanner."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pytest

from scripts.scan_independence import (
    Violation,
    load_forbidden_names,
    scan_paths,
    scan_repository,
    scan_text,
)

FORBIDDEN = ["cukd-xai", "tinyrf-kd"]


# --- scan_text ---


@pytest.mark.parametrize(
    "text,expected",
    [
        ("clean text", 0),
        ("C:\\path", 1),
        ("C:/path", 1),
        ("C:\\a\nD:/b", 2),
    ],
)
def test_scan_text_drive(text, expected):
    drive = [
        v for v in scan_text(text, "f", []) if v.rule == "drive_letter_absolute_path"
    ]
    assert len(drive) == expected


def test_scan_text_forbidden_substring():
    """Identifier substring is detected even with prefix/suffix."""
    v = scan_text("project CuKD-XAI-backup file", "f", FORBIDDEN)
    assert any(x.rule == "forbidden_name" and x.matched == "cukd-xai" for x in v)


def test_scan_text_violation_attrs():
    v = scan_text("C:\\p", "f", [])[0]
    assert v.path == "f" and v.line == 1 and v.rule == "drive_letter_absolute_path"


# --- scan_paths ---


def test_scan_paths_clean(tmp_path: Path):
    f = tmp_path / "c.txt"
    f.write_text("ok")
    assert scan_paths([str(f)], []) == []


def test_scan_paths_drive(tmp_path: Path):
    f = tmp_path / "b.txt"
    f.write_text("C:\\p")
    assert len(scan_paths([str(f)], [])) == 1


def test_scan_paths_missing(tmp_path: Path):
    with pytest.raises(RuntimeError, match="File not found"):
        scan_paths([str(tmp_path / "x.txt")], [])


def test_scan_paths_bad_utf8(tmp_path: Path):
    f = tmp_path / "b.txt"
    f.write_bytes(b"\x80")
    with pytest.raises(RuntimeError, match="Not UTF-8"):
        scan_paths([str(f)], [])


# --- suffix exclusions ---


@pytest.mark.parametrize(
    "path",
    [
        "configs/forbidden-terms.txt",
        "tests/contract/test_repository_independence.py",
        "docs/superpowers/p.md",
        "a/b/docs/superpowers/c.md",
    ],
)
def test_scan_paths_suffix_excluded(tmp_path: Path, path: str):
    f = tmp_path / path
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("C:\\p")
    assert scan_paths([str(f)], []) == []


def test_scan_paths_binary_excluded(tmp_path: Path):
    f = tmp_path / "x.png"
    f.write_bytes(b"C:\\p")
    assert scan_paths([str(f)], []) == []


# --- scan_repository ---


def test_scan_repo_injected(tmp_path: Path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "t.py").write_text("C:\\p")
    called = []

    def prov() -> Sequence[str]:
        called.append(1)
        return ["src/t.py"]

    v = scan_repository([], prov, tmp_path)
    assert called and len(v) == 1 and v[0].path == "src/t.py"


def test_scan_repo_out_of_scope(tmp_path: Path):
    (tmp_path / "x").mkdir()
    (tmp_path / "x" / "t.txt").write_text("C:\\p")

    def prov() -> Sequence[str]:
        return ["x/t.txt"]

    assert scan_repository([], prov, tmp_path) == []


def test_scan_repo_missing_tracked(tmp_path: Path):
    def prov() -> Sequence[str]:
        return ["src/x.py"]

    with pytest.raises(RuntimeError, match="Tracked file not found"):
        scan_repository([], prov, tmp_path)


# --- unsafe tracked paths ---


@pytest.mark.parametrize(
    "path,msg",
    [
        ("/abs/path", "Absolute tracked path"),
        ("C:\\win", "Absolute tracked path"),
        ("../escape", "Parent traversal"),
        ("a/../b", "Parent traversal"),
        ("", "Empty tracked path"),
    ],
)
def test_scan_repo_unsafe_paths(tmp_path: Path, path, msg):
    with pytest.raises(RuntimeError, match=msg):
        scan_repository([], lambda: [path], tmp_path)


# --- provider errors ---


def test_scan_repo_provider_error_wrapped(tmp_path: Path):
    with pytest.raises(RuntimeError, match="Provider failed"):
        scan_repository([], lambda: (_ for _ in ()).throw(ValueError("e")), tmp_path)


# --- load_forbidden_names ---


def test_load_forbidden(tmp_path: Path):
    f = tmp_path / "c.txt"
    f.write_text("# comment\nCuKD-XAI\n")
    assert load_forbidden_names(str(f)) == ["cukd-xai"]


def test_load_forbidden_missing(tmp_path: Path):
    with pytest.raises(RuntimeError, match="Config not found"):
        load_forbidden_names(str(tmp_path / "x.txt"))


def test_load_forbidden_bad_utf8(tmp_path: Path):
    f = tmp_path / "c.txt"
    f.write_bytes(b"\x80")
    with pytest.raises(RuntimeError, match="Config not UTF-8"):
        load_forbidden_names(str(f))


# --- Violation frozen ---


def test_violation_frozen():
    v = Violation("p", 1, "r", "m")
    with pytest.raises(AttributeError):
        v.line = 2  # type: ignore


# --- git ls-files -z ---


def test_git_ls_files_nul_parsing(monkeypatch, tmp_path: Path):
    """Test NUL-separated git ls-files output parsing."""
    from scripts import scan_independence as si

    class R:
        stdout = b"src/a.py\0src/b with space.py\0"
        check = True

    monkeypatch.setattr(si.subprocess, "run", lambda *a, **kw: R())
    files = si.get_tracked_files()
    assert files == ["src/a.py", "src/b with space.py"]


def test_git_ls_files_empty_fails(monkeypatch, tmp_path: Path):
    """Test empty listing raises error."""
    from scripts import scan_independence as si

    monkeypatch.setattr(
        si.subprocess,
        "run",
        lambda *a, **kw: type("R", (), {"stdout": b"", "check": True})(),
    )
    with pytest.raises(RuntimeError, match="no files"):
        si.get_tracked_files()


def test_git_ls_files_failure(monkeypatch, tmp_path: Path):
    """Test git command failure raises error."""
    from scripts import scan_independence as si

    def fail(*a, **kw):
        raise si.subprocess.CalledProcessError(1, "git", stderr=b"error")

    monkeypatch.setattr(si.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="git ls-files failed"):
        si.get_tracked_files()


def test_git_ls_files_invalid_utf8(monkeypatch, tmp_path: Path):
    """Test invalid UTF-8 in NUL output raises error."""
    from scripts import scan_independence as si

    monkeypatch.setattr(
        si.subprocess,
        "run",
        lambda *a, **kw: type(
            "R", (), {"stdout": b"src/\x80bad.py\0", "check": True}
        )(),
    )
    with pytest.raises(RuntimeError, match="invalid UTF-8"):
        si.get_tracked_files()


# --- main() CLI ---


def test_main_clean(monkeypatch, tmp_path: Path):
    """Test main() returns 0 for clean repository."""
    from scripts import scan_independence as si

    monkeypatch.setattr(si, "get_tracked_files", lambda: ["README.md"])
    assert si.main() == 0


# --- .gitignore verification ---


def test_gitignore_env_example_unignored():
    """Verify .env.example is not ignored."""
    content = Path(".gitignore").read_text(encoding="utf-8")
    lines = content.splitlines()
    assert "!.env.example" in lines
    assert any(".env.*" in ln and "!" not in ln for ln in lines)


def test_gitignore_reports_not_ignored():
    """Verify root reports/ is not ignored."""
    content = Path(".gitignore").read_text(encoding="utf-8")
    lines = [
        ln.strip()
        for ln in content.splitlines()
        if ln.strip() and not ln.startswith("#")
    ]
    # Root reports/ should NOT be in ignore list
    assert "reports/" not in lines
    # Only runtime/generated should be ignored
    assert "reports/runtime/" in lines or "reports/generated/" in lines


def test_gitignore_docs_results_trackable():
    """Verify docs/results is not ignored."""
    content = Path(".gitignore").read_text(encoding="utf-8")
    # Should not have broad docs/ ignore
    assert "docs/" not in content or "docs/results" in content

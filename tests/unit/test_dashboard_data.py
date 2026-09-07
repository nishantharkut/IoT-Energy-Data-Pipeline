from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dashboard.data import REQUIRED_VIEWS, load_finalized_run


def _write_run(tmp_path: Path, **changes) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)
    artifact = run_dir / "gold-aggregates.ndjson"
    artifact.write_text('{"count":1}\n', encoding="utf-8")
    views = {
        name: {"title": name.replace("_", " ").title(), "rows": []}
        for name in REQUIRED_VIEWS
    }
    manifest = {
        "schema_version": "finalized-run-v1",
        "status": "finalized",
        "reconciled": True,
        "immutable": True,
        "replay_run_id": "fixture-clean",
        "artifacts": {
            "gold_aggregates": {
                "relative_path": "gold-aggregates.ndjson",
                "byte_size": artifact.stat().st_size,
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            }
        },
        "views": views,
    }
    manifest.update(changes)
    manifest_path = run_dir / "run-manifest.json"
    payload = (
        json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=True) + "\n"
    ).encode()
    manifest_path.write_bytes(payload)
    (run_dir / "run-manifest.sha256").write_text(
        hashlib.sha256(payload).hexdigest() + "\n", encoding="ascii"
    )
    return run_dir


def test_dashboard_loads_only_hash_verified_finalized_views(tmp_path: Path) -> None:
    run = load_finalized_run(_write_run(tmp_path))

    assert run["replay_run_id"] == "fixture-clean"
    assert set(run["views"]) == set(REQUIRED_VIEWS)


@pytest.mark.parametrize(
    "change,message",
    [
        ({"status": "running"}, "not finalized"),
        ({"reconciled": False}, "reconciliation"),
        ({"immutable": False}, "immutable"),
        ({"schema_version": "future-v9"}, "schema"),
        ({"views": {}}, "dashboard views"),
    ],
)
def test_dashboard_rejects_unpublishable_run(
    tmp_path: Path, change: dict, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        load_finalized_run(_write_run(tmp_path, **change))


def test_dashboard_rejects_manifest_or_artifact_mutation(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    (run_dir / "gold-aggregates.ndjson").write_text("changed\n", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact integrity"):
        load_finalized_run(run_dir)

    run_dir = _write_run(tmp_path / "second")
    manifest = run_dir / "run-manifest.json"
    manifest.write_text(manifest.read_text("utf-8") + " ", encoding="utf-8")
    with pytest.raises(ValueError, match="manifest integrity"):
        load_finalized_run(run_dir)


def test_dashboard_rejects_files_added_after_sealing(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    (run_dir / "unregistered.txt").write_text("added later", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact registry does not match"):
        load_finalized_run(run_dir)


def test_dashboard_rejects_unsafe_artifact_path(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    path = run_dir / "run-manifest.json"
    manifest = json.loads(path.read_text("utf-8"))
    manifest["artifacts"]["gold_aggregates"]["relative_path"] = "../outside"
    payload = (json.dumps(manifest, sort_keys=True, indent=2) + "\n").encode()
    path.write_bytes(payload)
    (run_dir / "run-manifest.sha256").write_text(
        hashlib.sha256(payload).hexdigest() + "\n", encoding="ascii"
    )

    with pytest.raises(ValueError, match="unsafe artifact path"):
        load_finalized_run(run_dir)

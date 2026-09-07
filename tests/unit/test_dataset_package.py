from __future__ import annotations

import hashlib
import json
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile, ZipInfo

import pytest

from iot_energy_pipeline.dataset_package import (
    DryadPackageContract,
    verify_dryad_package,
)


def _contract(inner: bytes, readme: bytes) -> DryadPackageContract:
    return DryadPackageContract(
        dataset_doi="10.5061/dryad.test",
        dataset_version="test-v1",
        inner_archive_size=len(inner),
        inner_archive_sha256=hashlib.sha256(inner).hexdigest(),
        readme_size=len(readme),
    )


def _package(path: Path, inner: bytes, readme: bytes) -> None:
    with ZipFile(path, "w", compression=ZIP_STORED) as archive:
        archive.writestr("All_Data.zip", inner)
        archive.writestr("README.md", readme)


def test_package_verification_streams_and_seals_official_files(
    tmp_path: Path,
) -> None:
    inner = b"small synthetic inner archive"
    readme = b"fixture readme\n"
    package = tmp_path / "doi-package.zip"
    _package(package, inner, readme)

    report = verify_dryad_package(
        package,
        tmp_path / "report.json",
        contract=_contract(inner, readme),
    )

    assert report["status"] == "verified"
    assert report["container"]["file_name"] == "doi-package.zip"
    assert report["files"]["All_Data.zip"] == {
        "byte_size": len(inner),
        "sha256": hashlib.sha256(inner).hexdigest(),
    }
    assert report["files"]["README.md"]["byte_size"] == len(readme)
    assert json.loads((tmp_path / "report.json").read_text("utf-8")) == report
    assert str(tmp_path) not in (tmp_path / "report.json").read_text("utf-8")


def test_package_report_is_byte_reproducible(tmp_path: Path) -> None:
    inner = b"inner"
    readme = b"readme"
    package = tmp_path / "package.zip"
    _package(package, inner, readme)

    verify_dryad_package(
        package, tmp_path / "a.json", contract=_contract(inner, readme)
    )
    verify_dryad_package(
        package, tmp_path / "b.json", contract=_contract(inner, readme)
    )

    assert (tmp_path / "a.json").read_bytes() == (tmp_path / "b.json").read_bytes()


@pytest.mark.parametrize("changed", [b"wrong", b"inner-corrupted"])
def test_inner_archive_digest_mismatch_fails(tmp_path: Path, changed: bytes) -> None:
    package = tmp_path / "package.zip"
    _package(package, changed, b"readme")
    contract = _contract(b"expected", b"readme")

    with pytest.raises(ValueError, match="All_Data.zip integrity mismatch"):
        verify_dryad_package(package, tmp_path / "report.json", contract=contract)

    assert not (tmp_path / "report.json").exists()


def test_package_membership_is_exact_and_duplicate_safe(tmp_path: Path) -> None:
    package = tmp_path / "package.zip"
    with ZipFile(package, "w", compression=ZIP_STORED) as archive:
        archive.writestr("All_Data.zip", b"inner")
        archive.writestr("README.md", b"readme")
        duplicate = ZipInfo("README.md")
        duplicate.compress_type = ZIP_STORED
        with pytest.warns(UserWarning, match="Duplicate name"):
            archive.writestr(duplicate, b"second")

    with pytest.raises(ValueError, match="exactly"):
        verify_dryad_package(
            package,
            tmp_path / "report.json",
            contract=_contract(b"inner", b"readme"),
        )


def test_output_is_immutable(tmp_path: Path) -> None:
    package = tmp_path / "package.zip"
    _package(package, b"inner", b"readme")
    output = tmp_path / "report.json"
    output.write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="immutable package report"):
        verify_dryad_package(
            package,
            output,
            contract=_contract(b"inner", b"readme"),
        )


def test_package_verification_is_available_through_cli(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = tmp_path / "package.zip"
    package.write_bytes(b"placeholder")
    output = tmp_path / "report.json"
    from iot_energy_pipeline import cli

    def fake_verify(package_arg: Path, output_arg: Path) -> dict[str, object]:
        assert package_arg == package.resolve()
        output_arg.write_text("{}\n", encoding="utf-8")
        return {"dataset_version": "fixture-v1"}

    monkeypatch.setattr(cli, "verify_dryad_package", fake_verify)

    result = cli.main(
        ["verify-package", "--package", str(package), "--output", str(output)]
    )

    assert result == 0
    assert output.exists()

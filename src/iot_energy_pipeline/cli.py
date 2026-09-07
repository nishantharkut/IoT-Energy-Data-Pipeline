"""Command-line interface for dataset registration and profiling."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from iot_energy_pipeline.canonicalize import CanonicalizationRules, build_snapshot
from iot_energy_pipeline.dataset_package import verify_dryad_package
from iot_energy_pipeline.manifests import DatasetManifest, register_dataset
from iot_energy_pipeline.profile import profile_dataset
from iot_energy_pipeline.source_audit import audit_source_telemetry
from iot_energy_pipeline.source_layout import SourceLayoutRules


def _display_path(path_arg: str) -> str:
    """Get display path - try to make it relative to cwd, otherwise use filename."""
    from os.path import relpath

    try:
        # Try to make it relative to current working directory
        relative = relpath(path_arg)
        # If it's still absolute or goes up directories, just use the filename
        if relative.startswith("..") or Path(relative).is_absolute():
            return Path(path_arg).name
        return relative
    except (ValueError, OSError):
        # Fallback to just the filename
        return Path(path_arg).name


def _cmd_register(args: argparse.Namespace) -> int:
    """Handle the register command."""
    try:
        root = Path(args.root).resolve()
        output = Path(args.output).resolve()
        dataset_version = args.dataset_version
        source_variant = args.source_variant

        manifest = register_dataset(
            root=root,
            output=output,
            dataset_version=dataset_version,
            source_variant=source_variant,
        )

        print(f"Registered {len(manifest.files)} files")
        print(f"Dataset version: {manifest.dataset_version}")
        print(f"Source variant: {manifest.source_variant}")
        # Use display path to avoid printing absolute local paths
        print(f"Manifest written to: {_display_path(args.output)}")
        return 0

    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 2


def _cmd_verify_package(args: argparse.Namespace) -> int:
    """Verify the downloaded HKUST Dryad v5 wrapper without extracting it."""

    try:
        report = verify_dryad_package(
            Path(args.package).resolve(), Path(args.output).resolve()
        )
        print(f"Verified Dryad package: {report['dataset_version']}")
        print(f"Report written to: {_display_path(args.output)}")
        return 0
    except (ValueError, FileExistsError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 2


def _cmd_profile(args: argparse.Namespace) -> int:
    """Handle the profile command."""
    try:
        manifest_path = Path(args.manifest).resolve()
        root = Path(args.root).resolve()
        output = Path(args.output).resolve()

        manifest = DatasetManifest.from_json(manifest_path, root=root)

        profile = profile_dataset(manifest, output)

        print(f"Profiled {len(profile.workbooks)} workbooks")
        print(f"Dataset version: {profile.dataset_version}")
        # Use display path to avoid printing absolute local paths
        print(f"Profile written to: {_display_path(args.output)}")
        return 0

    except (ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 2


def _cmd_audit_source(args: argparse.Namespace) -> int:
    """Audit registered telemetry through an explicit source-layout file."""

    try:
        manifest = DatasetManifest.from_json(
            Path(args.manifest).resolve(),
            root=Path(args.root).resolve(),
        )
        layout = SourceLayoutRules.from_json(Path(args.layout).resolve())
        output = Path(args.output).resolve()
        report = audit_source_telemetry(manifest, layout, output)
        print(f"Audited {report['totals']['workbook_count']} workbooks")
        print(f"Dataset version: {report['dataset_version']}")
        print(f"Source variant: {report['source_variant']}")
        print(f"Audit written to: {_display_path(args.output)}")
        return 0
    except (ValueError, RuntimeError, FileExistsError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 2


def _cmd_canonicalize(args: argparse.Namespace) -> int:
    """Build an immutable snapshot from an explicit evidence contract."""

    try:
        manifest = DatasetManifest.from_json(
            Path(args.manifest).resolve(),
            root=Path(args.root).resolve(),
        )
        rules = CanonicalizationRules.from_json(Path(args.rules).resolve())
        result = build_snapshot(manifest, Path(args.output).resolve(), rules)
        print(f"Canonicalized {result.event_count} source events")
        print(f"Quarantined {result.source_quality_count} source rows")
        print(f"Snapshot written to: {_display_path(args.output)}")
        return 0
    except (ValueError, RuntimeError, FileExistsError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 2


def main(argv: list[str] | None = None) -> int:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        prog="iot_energy_pipeline",
        description="IoT Energy Data Pipeline - Dataset registration and profiling",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify_parser = subparsers.add_parser(
        "verify-package",
        help="Verify the downloaded HKUST Dryad v5 package without extraction",
    )
    verify_parser.add_argument("--package", required=True)
    verify_parser.add_argument("--output", required=True)
    verify_parser.set_defaults(func=_cmd_verify_package)

    # Register command
    register_parser = subparsers.add_parser(
        "register",
        help="Register dataset files",
    )
    register_parser.add_argument(
        "--root",
        required=True,
        help="Root directory containing source files",
    )
    register_parser.add_argument(
        "--output",
        required=True,
        help="Output path for manifest JSON",
    )
    register_parser.add_argument(
        "--dataset-version",
        required=True,
        help="Dataset version identifier",
    )
    register_parser.add_argument(
        "--source-variant",
        required=True,
        choices=["raw", "clean"],
        help="Source variant (raw or clean)",
    )
    register_parser.set_defaults(func=_cmd_register)

    # Profile command
    profile_parser = subparsers.add_parser(
        "profile",
        help="Profile registered dataset",
    )
    profile_parser.add_argument(
        "--manifest",
        required=True,
        help="Path to manifest JSON",
    )
    profile_parser.add_argument(
        "--root",
        required=True,
        help="Root directory containing source files",
    )
    profile_parser.add_argument(
        "--output",
        required=True,
        help="Output path for profile JSON",
    )
    profile_parser.set_defaults(func=_cmd_profile)

    audit_parser = subparsers.add_parser(
        "audit-source",
        help="Audit telemetry using a closed source-layout contract",
    )
    audit_parser.add_argument("--manifest", required=True)
    audit_parser.add_argument("--root", required=True)
    audit_parser.add_argument("--layout", required=True)
    audit_parser.add_argument("--output", required=True)
    audit_parser.set_defaults(func=_cmd_audit_source)

    canonicalize_parser = subparsers.add_parser(
        "canonicalize",
        help="Build an immutable snapshot from evidence-bound rules",
    )
    canonicalize_parser.add_argument("--manifest", required=True)
    canonicalize_parser.add_argument("--root", required=True)
    canonicalize_parser.add_argument("--rules", required=True)
    canonicalize_parser.add_argument("--output", required=True)
    canonicalize_parser.set_defaults(func=_cmd_canonicalize)

    args = parser.parse_args(argv)
    # argparse types func as Any, but we know it returns int
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

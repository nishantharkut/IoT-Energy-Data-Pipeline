from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_spark_stream_file_is_a_valid_spark_submit_entrypoint() -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path.cwd())
    result = subprocess.run(
        [sys.executable, "jobs/spark/stream.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--bootstrap-servers" in result.stdout
    assert "--max-offsets-per-trigger" in result.stdout


def test_scale_stream_does_not_force_the_canonical_snapshot_into_broadcast() -> None:
    source = Path("jobs/spark/stream.py").read_text(encoding="utf-8")

    assert "broadcast(canonical)" not in source


def test_spark_required_bindings_use_null_safe_comparisons() -> None:
    stream = Path("jobs/spark/stream.py").read_text(encoding="utf-8")
    finalizer = Path("jobs/spark/finalize_distributed.py").read_text(encoding="utf-8")

    assert stream.count("eqNullSafe") >= 4
    assert finalizer.count("eqNullSafe") >= 15


def test_spark_terminal_accounting_includes_empty_partitions() -> None:
    finalizer = Path("jobs/spark/finalize_distributed.py").read_text(encoding="utf-8")

    assert "spark.range(manifest.partition_count)" in finalizer
    assert 'fillna({"scheduled_partition_delivery_count": 0})' in finalizer


def test_spark_batch_finalizer_is_a_valid_spark_submit_entrypoint() -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path.cwd())
    result = subprocess.run(
        [sys.executable, "jobs/spark/finalize_distributed.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--run-dir" in result.stdout
    assert "--layers-root" in result.stdout


def test_hadoop_oracle_runner_is_a_valid_container_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "jobs/hadoop/distributed_oracle.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--canonical-ndjson" in result.stdout
    assert "--output-dir" in result.stdout


def test_spark_layer_auditor_is_a_valid_spark_submit_entrypoint() -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path.cwd())
    result = subprocess.run(
        [sys.executable, "jobs/spark/audit_layers.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--replay-manifest-hash" in result.stdout
    assert "--output-root" in result.stdout


def test_layer_audit_writes_to_the_directory_consumed_by_reconciliation() -> None:
    source = Path("jobs/spark/audit_layers.py").read_text(encoding="utf-8")

    assert "audit_root = output_root" in source
    assert 'output_root / replay_manifest_hash / "audit"' not in source


def test_layer_audit_can_read_valid_empty_parquet_layers() -> None:
    source = Path("jobs/spark/audit_layers.py").read_text(encoding="utf-8")

    assert source.count("spark.read.schema(") >= 5
    assert "spark.read.parquet" not in source


def test_distributed_reconciliation_is_a_valid_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.run_reconciliation", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--oracle-dir" in result.stdout
    assert "--layer-audit-dir" in result.stdout


def test_distributed_business_report_is_a_valid_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.business_report", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--reconciliation" in result.stdout
    assert "--output" in result.stdout


def test_distributed_run_finalizer_is_a_valid_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.run_finalization", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--decision-report" in result.stdout
    assert "--machine-metadata" in result.stdout


def test_dataset_cli_exposes_evidence_bound_canonicalization() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.cli", "canonicalize", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--rules" in result.stdout
    assert "--manifest" in result.stdout
    assert "--root" in result.stdout


def test_generic_distributed_run_has_prepare_and_replay_entrypoints() -> None:
    prepare = subprocess.run(
        [
            sys.executable,
            "-m",
            "iot_energy_pipeline.distributed_run",
            "prepare",
            "--help",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    replay = subprocess.run(
        [
            sys.executable,
            "-m",
            "iot_energy_pipeline.distributed_run",
            "replay",
            "--help",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert prepare.returncode == 0, prepare.stderr
    assert "--snapshot-dir" in prepare.stdout
    assert "--event-count" in prepare.stdout
    assert replay.returncode == 0, replay.stderr
    assert "--bootstrap-servers" in replay.stdout
    assert "--resume" in replay.stdout


def test_experiment_plan_compiler_is_a_valid_safe_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.experiment_plan", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--matrix" in result.stdout
    assert "--snapshot-manifest" in result.stdout
    assert "--machine-metadata" in result.stdout
    assert "--output" in result.stdout


def test_spark_query_benchmark_is_a_valid_deferred_entrypoint() -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path.cwd())
    result = subprocess.run(
        [sys.executable, "jobs/spark/benchmark_queries.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--gold-dir" in result.stdout
    assert "--decision-report" in result.stdout
    assert "--queries-dir" in result.stdout
    assert "--repetitions" in result.stdout


def test_spark_storage_benchmark_is_a_valid_deferred_entrypoint() -> None:
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(Path.cwd())
    result = subprocess.run(
        [sys.executable, "jobs/spark/benchmark_storage.py", "--help"],
        text=True,
        capture_output=True,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    assert "--canonical-ndjson" in result.stdout
    assert "--output-root" in result.stdout
    assert "--run-id" in result.stdout


def test_experiment_executor_requires_explicit_confirmation_argument() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "iot_energy_pipeline.experiment_execution",
            "--help",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--experiment-plan" in result.stdout
    assert "--runtime-root" in result.stdout
    assert "--confirm" in result.stdout


def test_course_report_generator_is_a_valid_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "iot_energy_pipeline.course_report", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--experiment-results" in result.stdout
    assert "--runtime-root" in result.stdout
    assert "--output" in result.stdout

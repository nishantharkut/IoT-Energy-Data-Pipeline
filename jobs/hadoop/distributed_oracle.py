"""Run the independent mapper/reducer through real HDFS and YARN."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if check and result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"command failed ({result.returncode}): {detail}")
    return result


def _safe_run_id(run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id):
        raise ValueError("run_id is not safe for an HDFS experiment path")
    return run_id


def _validate_oracle_file(path: Path, *, key_field: str, label: str) -> int:
    """Validate sorted oracle output without retaining it in process memory."""

    previous: str | None = None
    count = 0
    with path.open("r", encoding="utf-8", newline="") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"oracle output line {line_number} is invalid JSON"
                ) from exc
            if not isinstance(record, dict):
                raise ValueError(f"oracle output line {line_number} is not an object")
            key = record.get(key_field)
            if not isinstance(key, str) or not key:
                raise ValueError(
                    f"{label} output line {line_number} has no {key_field}"
                )
            if previous is not None and key <= previous:
                raise ValueError(f"{label} output keys are duplicate or unsorted")
            previous = key
            count += 1
    if count == 0:
        raise ValueError(f"{label} output is empty")
    return count


def _download_hdfs_output(hdfs_glob: str, destination: Path) -> None:
    """Stream an HDFS output directly to an immutable local file."""

    with destination.open("xb") as output:
        result = subprocess.run(
            ["hdfs", "dfs", "-cat", hdfs_glob],
            stdout=output,
            stderr=subprocess.PIPE,
            check=False,
        )
        output.flush()
        os.fsync(output.fileno())
    if result.returncode:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"cannot stream HDFS oracle output: {detail}")


def _streaming_job(
    *,
    streaming_jar: Path,
    run_id: str,
    label: str,
    mapper: Path,
    reducer: Path,
    hdfs_input: str,
    hdfs_output: str,
) -> tuple[subprocess.CompletedProcess[str], float, str | None]:
    command = [
        "hadoop",
        "jar",
        str(streaming_jar),
        "-D",
        f"mapreduce.job.name=iot-energy-{label}-{run_id}",
        "-D",
        "mapreduce.job.reduces=1",
        "-files",
        f"{mapper},{reducer}",
        "-mapper",
        f"python3 {mapper.name}",
        "-reducer",
        f"python3 {reducer.name}",
        "-input",
        hdfs_input,
        "-output",
        hdfs_output,
    ]
    started = time.perf_counter()
    result = _run(command, check=False)
    elapsed = time.perf_counter() - started
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Hadoop {label} job failed: {detail}")
    job_ids = re.findall(r"job_[0-9]+_[0-9]+", result.stdout + "\n" + result.stderr)
    return result, elapsed, (job_ids[-1] if job_ids else None)


def run_distributed_oracle(
    *,
    run_id: str,
    canonical_ndjson: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Submit independent Hadoop Streaming code and seal its result locally."""

    run_id = _safe_run_id(run_id)
    if not canonical_ndjson.is_file():
        raise FileNotFoundError(f"canonical NDJSON does not exist: {canonical_ndjson}")
    if output_dir.exists():
        raise FileExistsError(f"immutable oracle output already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    hdfs_root = f"/coursework/{run_id}"
    hdfs_input = f"{hdfs_root}/input/canonical.ndjson"
    hdfs_aggregate_output = f"{hdfs_root}/aggregate-oracle-output"
    hdfs_source_output = f"{hdfs_root}/source-record-oracle-output"
    exists = _run(["hdfs", "dfs", "-test", "-e", hdfs_root], check=False)
    if exists.returncode == 0:
        raise FileExistsError(f"immutable HDFS run already exists: {hdfs_root}")
    if exists.returncode != 1:
        raise RuntimeError(f"cannot inspect HDFS run path: {exists.stderr.strip()}")

    source_sha256 = _sha256(canonical_ndjson)
    _run(["hdfs", "dfs", "-mkdir", "-p", f"{hdfs_root}/input"])
    _run(["hdfs", "dfs", "-put", str(canonical_ndjson), hdfs_input])
    checksum_result = _run(["hdfs", "dfs", "-checksum", hdfs_input])
    checksum_fields = checksum_result.stdout.strip().split()
    if len(checksum_fields) < 3:
        raise ValueError("HDFS did not return an input checksum")
    streaming_jars = sorted(
        Path("/opt/hadoop/share/hadoop/tools/lib").glob("hadoop-streaming-*.jar")
    )
    if len(streaming_jars) != 1:
        raise ValueError("expected exactly one Hadoop Streaming JAR")
    aggregate_mapper = Path("/opt/pipeline/jobs/hadoop/mapper.py")
    aggregate_reducer = Path("/opt/pipeline/jobs/hadoop/reducer.py")
    source_mapper = Path("/opt/pipeline/jobs/hadoop/record_mapper.py")
    source_reducer = Path("/opt/pipeline/jobs/hadoop/record_reducer.py")
    aggregate_job, aggregate_seconds, aggregate_job_id = _streaming_job(
        streaming_jar=streaming_jars[0],
        run_id=run_id,
        label="aggregate-oracle",
        mapper=aggregate_mapper,
        reducer=aggregate_reducer,
        hdfs_input=hdfs_input,
        hdfs_output=hdfs_aggregate_output,
    )
    source_job, source_seconds, source_job_id = _streaming_job(
        streaming_jar=streaming_jars[0],
        run_id=run_id,
        label="source-record-oracle",
        mapper=source_mapper,
        reducer=source_reducer,
        hdfs_input=hdfs_input,
        hdfs_output=hdfs_source_output,
    )
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        aggregate_path = stage / "hadoop-aggregates.ndjson"
        source_path = stage / "hadoop-source-records.ndjson"
        _download_hdfs_output(f"{hdfs_aggregate_output}/part-*", aggregate_path)
        _download_hdfs_output(f"{hdfs_source_output}/part-*", source_path)
        aggregate_count = _validate_oracle_file(
            aggregate_path,
            key_field="group_key",
            label="aggregate oracle",
        )
        source_count = _validate_oracle_file(
            source_path,
            key_field="source_event_id",
            label="source-record oracle",
        )
        for label, result in (
            ("aggregate", aggregate_job),
            ("source-record", source_job),
        ):
            (stage / f"hadoop-{label}-job.stdout.log").write_text(
                result.stdout, encoding="utf-8", newline="\n"
            )
            (stage / f"hadoop-{label}-job.stderr.log").write_text(
                result.stderr, encoding="utf-8", newline="\n"
            )
        manifest: dict[str, Any] = {
            "schema_version": "hadoop-oracle-manifest-v2",
            "status": "complete",
            "run_id": run_id,
            "implementation": {
                "aggregate_mapper": str(aggregate_mapper),
                "aggregate_reducer": str(aggregate_reducer),
                "source_record_mapper": str(source_mapper),
                "source_record_reducer": str(source_reducer),
                "imports_spark_code": False,
            },
            "input": {
                "local_path": str(canonical_ndjson),
                "local_sha256": source_sha256,
                "hdfs_uri": f"hdfs://hdfs:9000{hdfs_input}",
                "hdfs_checksum_algorithm": checksum_fields[-2],
                "hdfs_checksum": checksum_fields[-1],
            },
            "outputs": {
                "aggregates": {
                    "hdfs_uri": f"hdfs://hdfs:9000{hdfs_aggregate_output}",
                    "local_file": "hadoop-aggregates.ndjson",
                    "record_count": aggregate_count,
                    "byte_size": aggregate_path.stat().st_size,
                    "sha256": _sha256(aggregate_path),
                },
                "source_records": {
                    "hdfs_uri": f"hdfs://hdfs:9000{hdfs_source_output}",
                    "local_file": "hadoop-source-records.ndjson",
                    "record_count": source_count,
                    "byte_size": source_path.stat().st_size,
                    "sha256": _sha256(source_path),
                },
            },
            "jobs": {
                "aggregate": {
                    "hadoop_job_id": aggregate_job_id,
                    "verification_seconds": aggregate_seconds,
                },
                "source_records": {
                    "hadoop_job_id": source_job_id,
                    "verification_seconds": source_seconds,
                },
            },
            "verification_seconds": aggregate_seconds + source_seconds,
            "reducers": 1,
        }
        _write_json(stage / "hadoop-oracle-manifest.json", manifest)
        if output_dir.exists():
            raise FileExistsError(
                f"immutable oracle output already exists: {output_dir}"
            )
        stage.replace(output_dir)
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--canonical-ndjson", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = run_distributed_oracle(
        run_id=args.run_id,
        canonical_ndjson=args.canonical_ndjson,
        output_dir=args.output_dir,
    )
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()

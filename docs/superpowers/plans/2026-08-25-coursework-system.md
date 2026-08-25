# IoT Energy Data Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible IoTBDM coursework pipeline that replays public HKUST smart-meter records through Kafka, processes them with Spark, verifies aggregates independently with Hadoop, and publishes finalized analytics without any dependency on prior intrusion-detection research.

**Architecture:** A dataset registration and profiling stage creates a canonical Parquet and NDJSON snapshot with deterministic source identities and exact scaled numeric values. Kafka replays deterministic delivery schedules into a Spark Bronze archive, a watermark-bounded Silver view, and quarantine. A deterministic batch pass builds Gold from Bronze, while Hadoop independently aggregates the canonical NDJSON from HDFS for field-level reconciliation.

**Tech Stack:** Python 3.11+, PyArrow, openpyxl, RDFLib, Kafka, PySpark, Hadoop Streaming, HDFS, Parquet, Streamlit, pytest, Hypothesis, Ruff, mypy, Docker Compose.

---

## File Structure

```text
IoT-Energy-Data-Pipeline/
  pyproject.toml
  README.md
  AGENTS.md
  compose.yaml
  .env.example
  .gitignore
  configs/
    experiments.toml
    forbidden-terms.txt
  schemas/
    canonical-event-v1.schema.json
    replay-manifest-v1.schema.json
  src/iot_energy_pipeline/
    cli.py
    contracts.py
    identity.py
    manifests.py
    profile.py
    canonicalize.py
    brick.py
    replay.py
    schedule.py
    reports.py
  jobs/spark/
    common.py
    stream.py
    finalize.py
    aggregates.py
  jobs/hadoop/
    mapper.py
    reducer.py
    reconcile.py
  infra/
    spark/Dockerfile
    hadoop/Dockerfile
    hadoop/conf/
  dashboard/
    app.py
    data.py
  queries/
    energy_summary.sql
    data_quality.sql
  scripts/
    bootstrap.ps1
    bootstrap.sh
    run_fixture.ps1
    run_fixture.sh
    run_experiments.ps1
    run_experiments.sh
    scan_independence.py
  tests/
    unit/
    contract/
    property/
    integration/
    fixtures/hkust-mini/
  docs/
    dataset.md
    experiment-protocol.md
    reproducibility.md
    course-concepts.md
    results/course-results.md
```

The synthetic `hkust-mini` fixture contains no copied dataset values. It has
three fictional meters, two fictional buildings, 48 regular timestamps, one
null numeric cell, one repeated meter timestamp, one zero interval, one
unmatched Brick meter, and a separate malformed-TTL fixture. Test factories
generate its workbooks and TTL files deterministically.

### Task 1: Repository Foundation and Independence Gate

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `AGENTS.md`
- Create: `README.md`
- Create: `configs/forbidden-terms.txt`
- Create: `scripts/scan_independence.py`
- Test: `tests/contract/test_repository_independence.py`
- Test: `tests/unit/test_package.py`

- [ ] **Step 1: Write the package and independence tests**

The package test imports `iot_energy_pipeline`. The independence test scans
tracked application code, user documentation, queries, schemas, and reports.
It rejects absolute drive paths and configured prior-project names. Only the
scanner pattern file and scanner tests are excluded.

- [ ] **Step 2: Run the tests and verify the expected import failure**

Run: `py -3.11 -m pytest tests/unit/test_package.py tests/contract/test_repository_independence.py -q`

Expected: package import fails because the package does not yet exist.

- [ ] **Step 3: Add the minimal package and tooling configuration**

Define Python 3.11 support and dependencies in separate `dev`, `dataset`,
`kafka`, `spark`, and `dashboard` extras. Configure Ruff and mypy. Ignore
datasets, Parquet, checkpoints, Kafka data, HDFS volumes, reports, secrets, and
virtual environments.

- [ ] **Step 4: Run foundation checks**

Run: `py -3.11 -m pytest tests/unit/test_package.py tests/contract/test_repository_independence.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit the foundation**

```text
git add .
git commit -m "chore: establish independent coursework repository"
```

### Task 2: Local Dataset Registration and Profiling

**Budget:** 6 hours. **Priority:** P1.

**Files:**
- Create: `src/iot_energy_pipeline/manifests.py`
- Create: `src/iot_energy_pipeline/profile.py`
- Create: `src/iot_energy_pipeline/cli.py`
- Create: `tests/fixtures/hkust-mini/README.md`
- Create: `tests/unit/test_manifests.py`
- Create: `tests/unit/test_profile.py`
- Create: `docs/dataset.md`

- [ ] **Step 1: Write failing registration tests**

Cover deterministic relative-path ordering, SHA-256 calculation, refusal of
missing files, workbook sheet and header discovery, timestamp and numeric text
profiling, explicit raw or clean source variants, and the absence of guessed
units. The official DOI, manual download steps, expected top-level source
layout, local registration command, and data-exclusion rule belong in
`docs/dataset.md`.

- [ ] **Step 2: Verify the failures**

Run: `py -3.11 -m pytest tests/unit/test_manifests.py tests/unit/test_profile.py -q`

Expected: imports fail for the unimplemented registration API.

- [ ] **Step 3: Implement registration and profiling**

Expose these stable functions:

```python
def register_dataset(root: Path, output: Path) -> DatasetManifest: ...
def profile_dataset(manifest: DatasetManifest, output: Path) -> DatasetProfile: ...
```

The profiler records source structure and observed statistics. It assigns
`UNKNOWN` measurement semantics unless official metadata and profile rules
prove a unit and measurement kind. Registration takes an explicit
`source_variant` and refuses to mix raw and clean paths.

- [ ] **Step 4: Verify deterministic output**

Run the profiler twice over the mini fixture and assert byte-identical JSON.

- [ ] **Step 5: Commit dataset registration**

```text
git add src tests docs/dataset.md
git commit -m "feat(dataset): register and profile HKUST source files"
```

### Task 3: Canonical Contract, Time, and Numeric Representation

**Budget:** 5 hours. **Priority:** P1.

**Files:**
- Create: `src/iot_energy_pipeline/contracts.py`
- Create: `src/iot_energy_pipeline/identity.py`
- Create: `schemas/canonical-event-v1.schema.json`
- Create: `tests/contract/test_canonical_schema.py`
- Create: `tests/unit/test_identity.py`
- Create: `tests/property/test_scaled_values.py`

- [ ] **Step 1: Write failing contract and property tests**

Test that identical values in separate source rows receive separate source
identifiers, repeated conversion is deterministic, timestamps retain original
text, Hong Kong timestamps convert to UTC only under a declared timezone, and
scaled integer conversion either preserves the Decimal exactly or fails.
Include explicit signed 64-bit overflow failures and prove that changing meter,
time, or value does not change an identifier when the source locator is fixed.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/contract/test_canonical_schema.py tests/unit/test_identity.py tests/property/test_scaled_values.py -q`

Expected: contract modules are missing.

- [ ] **Step 3: Implement the contract**

Expose:

```python
def source_event_id(locator: SourceLocator) -> str: ...
def logical_measurement_key(meter_id: str, event_time_utc: datetime) -> str: ...
def to_scaled_int(text: str, scale: int) -> int: ...
def canonicalize_time(text: str, source_zone: ZoneInfo) -> datetime: ...
```

Use `Decimal` and explicit signed 64-bit bounds. Never derive source identity
from meter, timestamp, or value. Define `SourceLocator` as a frozen structure
containing dataset version, source variant, relative path, sheet, and physical
row number. Use SHA-256 and fail if two locators produce one identifier.

- [ ] **Step 4: Run contract and property tests**

Expected: all tests pass over boundary values and generated decimal strings.

- [ ] **Step 5: Commit the canonical contract**

```text
git add src schemas tests
git commit -m "feat(contract): define exact smart-meter event semantics"
```

### Task 4: Brick Join and Canonical Snapshot

**Budget:** 7 hours. **Priority:** P1 with narrow metadata scope.

**Files:**
- Create: `src/iot_energy_pipeline/brick.py`
- Create: `src/iot_energy_pipeline/canonicalize.py`
- Create: `tests/unit/test_brick.py`
- Create: `tests/integration/test_canonical_snapshot.py`

- [ ] **Step 1: Write failing adapter tests**

Use a synthetic workbook and TTL fixture. Cover matched and unmatched meters,
duplicate source timestamps, multiple Brick matches, malformed TTL, null
values, invalid numeric text, deterministic row order, Parquet schema, NDJSON
equality, and snapshot manifest hashes. Verify that unmatched meters remain
valid with null enrichment. Verify that raw and clean variants cannot share a
snapshot. Verify that interpolation, imputation, clipping, and guessed units
are not available transformations.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_brick.py tests/integration/test_canonical_snapshot.py -q`

- [ ] **Step 3: Implement the minimum adapter**

Generate one canonical event per valid cleaned source row. Write invalid rows to
a source-quality report rather than silently dropping them. Produce Parquet,
canonical NDJSON, and an immutable snapshot manifest.

The snapshot manifest records file, TTL, and manifest SHA-256 values, detected
Brick version, join cardinality report, source variant, source locator fields,
and a measurement-kind/unit/scale registry.

- [ ] **Step 4: Verify byte-stable manifests and record equality**

Run the fixture snapshot twice and compare sorted event records and file hashes.

- [ ] **Step 5: Commit canonicalization**

```text
git add src tests
git commit -m "feat(dataset): build provenance-bound canonical snapshots"
```

### Task 5: Deterministic Replay Schedules and Kafka Receipts

**Budget:** 7 hours. **Priority:** P1.

**Files:**
- Create: `src/iot_energy_pipeline/schedule.py`
- Create: `src/iot_energy_pipeline/replay.py`
- Create: `schemas/replay-manifest-v1.schema.json`
- Create: `tests/unit/test_schedule.py`
- Create: `tests/contract/test_replay_manifest.py`
- Create: `tests/integration/test_kafka_replay.py`

- [ ] **Step 1: Write failing schedule tests**

Cover clean, duplicate, delayed, malformed, and combined schedules. Assert that
fault injection never removes the valid delivery of a source event, duplicate
deliveries have distinct delivery IDs, and identical seed/configuration inputs
produce the same schedule. Assert deterministic partition assignment using the
first eight SHA-256 bytes of `source_event_id` modulo `partition_count`. Store
the replay seed and resolved schedule hash in the run manifest.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_schedule.py tests/contract/test_replay_manifest.py -q`

- [ ] **Step 3: Implement schedules and receipts**

Write the manifest before replay. Use Kafka acknowledgements. Emit one terminal
record per partition. Write the receipt only after flush and include delivery
counts plus terminal offsets.

- [ ] **Step 4: Run unit tests and the Kafka fixture integration test**

Expected: produced key/value/header records and receipt counts match the
schedule exactly.

- [ ] **Step 5: Commit replay support**

```text
git add src schemas tests
git commit -m "feat(replay): add deterministic at-least-once event replay"
```

### Task 6: Reproducible Container Platform

**Budget:** 6 hours. **Priority:** P1.

**Files:**
- Create: `compose.yaml`
- Create: `infra/spark/Dockerfile`
- Create: `infra/hadoop/Dockerfile`
- Create: `infra/hadoop/hadoop-entrypoint.sh`
- Create: `infra/hadoop/conf/core-site.xml`
- Create: `infra/hadoop/conf/hdfs-site.xml`
- Create: `infra/hadoop/conf/mapred-site.xml`
- Create: `infra/hadoop/conf/yarn-site.xml`
- Create: `scripts/bootstrap.ps1`
- Create: `scripts/bootstrap.sh`
- Test: `tests/contract/test_compose.py`

- [ ] **Step 1: Write the Compose contract test**

Require pinned images or digests, health checks, named volumes, a non-host
network, explicit resource settings, and no source-data directory baked into an
image.

- [ ] **Step 2: Verify the missing Compose failure**

Run: `py -3.11 -m pytest tests/contract/test_compose.py -q`

- [ ] **Step 3: Add Kafka, Spark, HDFS, and Hadoop services**

Use Docker Compose only. Keep one documented local profile and one designated
execution-machine profile. Do not add Kubernetes or cloud resources.

- [ ] **Step 4: Validate configuration and service health**

Run: `docker compose config --quiet`

Expected: exit code zero.

- [ ] **Step 5: Commit the platform**

```text
git add compose.yaml infra scripts tests/contract/test_compose.py
git commit -m "build: add reproducible Kafka Spark and Hadoop platform"
```

### Task 7: Spark Bronze, Silver, and Quarantine

**Budget:** 8 hours. **Priority:** P1.

**Files:**
- Create: `jobs/spark/common.py`
- Create: `jobs/spark/stream.py`
- Create: `tests/unit/test_spark_transformations.py`
- Create: `tests/integration/test_spark_stream.py`

- [ ] **Step 1: Write failing Spark transformation tests**

Assert that Bronze retains raw Kafka value, key, headers, partition, offset,
and ingestion timestamp. Assert that Silver parses valid events, applies the
declared watermark and bounded deduplication, and that malformed values enter a
finite quarantine taxonomy. Cover invalid JSON, schema mismatch, unknown source
ID, invalid timestamp, invalid numeric value, overflow, missing identity, unit
or scale violation, and run-manifest mismatch. Assert before replay that the
retaining watermark exceeds the schedule's computed maximum event-time
lateness.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_spark_transformations.py -q`

- [ ] **Step 3: Implement streaming layers**

Keep Bronze append-only. Treat late data as valid data, not quarantine. Bind
all sinks and checkpoints to the immutable replay manifest hash.

- [ ] **Step 4: Verify fixture counts**

For every schedule, assert:

```text
Bronze deliveries = parse-valid deliveries + quarantined deliveries
```

- [ ] **Step 5: Commit the Spark stream**

```text
git add jobs/spark tests
git commit -m "feat(spark): process Bronze Silver and quarantine layers"
```

### Task 8: Exact Finalization and Gold Aggregates

**Budget:** 6 hours. **Priority:** P1.

**Files:**
- Create: `jobs/spark/aggregates.py`
- Create: `jobs/spark/finalize.py`
- Create: `tests/unit/test_gold_aggregates.py`
- Create: `tests/integration/test_finalization.py`

- [ ] **Step 1: Write failing finalization tests**

Cover receipt mismatch, missing terminal offsets, duplicate delivery IDs,
unknown source IDs, incomplete source coverage, immutable output refusal, and
exact recovery of canonical counts and scaled sums from fault-injected Bronze.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_gold_aggregates.py tests/integration/test_finalization.py -q`

- [ ] **Step 3: Implement batch Gold from Bronze**

Parse Bronze again, retain one valid delivery per source event, and aggregate
by declared unit and scale. Never build final Gold from bounded Silver.

A valid delivery must match the canonical event payload digest. When multiple
valid deliveries remain, retain the delivery with the lowest `(partition,
offset)` tuple. The selected payload is therefore deterministic and equivalent
to the canonical event.

- [ ] **Step 4: Verify clean and combined-fault fixtures**

Expected: both produce identical finalized source coverage and Gold aggregates.

- [ ] **Step 5: Commit exact finalization**

```text
git add jobs/spark tests
git commit -m "feat(spark): finalize exact Gold results from Bronze"
```

### Task 9: Independent Hadoop Oracle and Reconciliation

**Budget:** 7 hours. **Priority:** P1.

**Files:**
- Create: `jobs/hadoop/mapper.py`
- Create: `jobs/hadoop/reducer.py`
- Create: `jobs/hadoop/reconcile.py`
- Create: `tests/unit/test_hadoop_oracle.py`
- Create: `tests/unit/test_reconciliation.py`
- Create: `tests/integration/test_hadoop_streaming.py`
- Create: `tests/contract/test_oracle_independence.py`

- [ ] **Step 1: Write failing oracle tests**

Use canonical NDJSON directly. Verify count, signed scaled sum, minimum and
maximum event time, quality-flag count, deterministic sort order, and precise
field-level mismatch messages.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_hadoop_oracle.py tests/unit/test_reconciliation.py -q`

- [ ] **Step 3: Implement independent mapper and reducer**

Do not import Spark modules or consume Gold as oracle input. Allow only shared
JSON schema constants, not shared aggregation implementation.

Add an import-boundary test that fails when Hadoop code imports `jobs.spark`.
The mapper and reducer must also run as ordinary local Python processes for fast
development tests. The final coursework demonstration still requires the
HDFS/Hadoop execution and may not substitute the local runner for it.

- [ ] **Step 4: Run local and container oracle tests**

Expected: clean and fault-injected Spark Gold match the same canonical Hadoop
oracle. A deliberately altered aggregate must fail reconciliation.

- [ ] **Step 5: Commit the oracle**

```text
git add jobs/hadoop tests
git commit -m "feat(verification): reconcile Spark with Hadoop oracle"
```

### Task 10: SQL Analytics and Finalized Dashboard

**Budget:** 5 hours. **Priority:** P1.

**Files:**
- Create: `queries/energy_summary.sql`
- Create: `queries/data_quality.sql`
- Create: `dashboard/data.py`
- Create: `dashboard/app.py`
- Create: `tests/unit/test_dashboard_data.py`

- [ ] **Step 1: Write failing dashboard data tests**

Require a finalized manifest and passing reconciliation. Reject incomplete,
unreconciled, or schema-incompatible run directories.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_dashboard_data.py -q`

- [ ] **Step 3: Implement six read-only views**

Implement provenance and status, throughput and layer counts, injected-fault
disposition, energy and quality summaries, storage and query measurements, and
reconciliation. The data-quality query reports
`logical_measurement_key` multiplicity without deleting records. Do not add job
control, authentication, forecasting, or alerts.

- [ ] **Step 4: Run dashboard tests and a local fixture smoke test**

Expected: finalized fixture opens and unfinished fixture is rejected.

- [ ] **Step 5: Commit analytics**

```text
git add queries dashboard tests
git commit -m "feat(analytics): publish finalized course metrics"
```

### Task 11: Experiment Harness and Measured Reports

**Budget:** 6 hours plus unattended execution. **Priority:** P1.

**Files:**
- Create: `configs/experiments.toml`
- Create: `src/iot_energy_pipeline/reports.py`
- Create: `scripts/run_experiments.ps1`
- Create: `scripts/run_experiments.sh`
- Create: `tests/unit/test_experiment_matrix.py`
- Create: `tests/unit/test_reports.py`

- [ ] **Step 1: Write failing experiment tests**

Require five one-run correctness configurations, three repetitions for each
resolved scale configuration, one recovery fixture, one retaining and one
restrictive watermark fixture, explicit machine metadata, and no result
publication before reconciliation. Resolve the last scale to
`min(5_000_000, full_count)` and remove duplicate sizes. At each resolved
scale, measure canonical NDJSON, unpartitioned Parquet, and partitioned
compressed Parquet bytes.

- [ ] **Step 2: Verify failures**

Run: `py -3.11 -m pytest tests/unit/test_experiment_matrix.py tests/unit/test_reports.py -q`

- [ ] **Step 3: Implement the fixed matrix and report builder**

Reports derive medians, minima, and maxima from immutable run manifests. They
must not infer missing measurements or contain manually entered results.

- [ ] **Step 4: Run fixture report generation**

Expected: fixture report is reproducible and labels all values as fixture
measurements rather than full-data results.

- [ ] **Step 5: Commit experiment support**

```text
git add configs src scripts tests
git commit -m "feat(experiments): define reproducible coursework measurements"
```

### Task 12: Documentation, CI, and End-to-End Verification

**Budget:** 7 hours. **Priority:** P1.

**Files:**
- Create: `docs/experiment-protocol.md`
- Create: `docs/reproducibility.md`
- Create: `docs/course-concepts.md`
- Create: `docs/results/course-results.md`
- Create: `scripts/run_fixture.ps1`
- Create: `scripts/run_fixture.sh`
- Create: `.github/workflows/ci.yml`
- Create: `tests/integration/test_acceptance_gates.py`
- Modify: `README.md`

- [ ] **Step 1: Write the fixture end-to-end acceptance test**

The script must register the mini source, build a canonical snapshot, execute a
combined-fault replay, finalize Gold, run the Hadoop oracle, reconcile, build a
report, and verify dashboard eligibility.

It must also run one interrupted/resumed fixture and one uninterrupted control.
Compare sorted Gold records, oracle records, reconciliation fields, and source
coverage. Do not compare runtime IDs, elapsed times, Kafka offsets, report
bytes, or Parquet file bytes.

- [ ] **Step 2: Add concise coursework documentation**

Document exact commands, expected outputs, concept mapping, claim boundaries,
and the fact that full-data results are generated only on the designated
machine.

- [ ] **Step 3: Add CI checks**

Run Ruff, formatting, mypy, unit, property, and contract tests on every push.
Keep Docker integration as a separately triggered workflow if CI resources are
insufficient.

The acceptance-gate integration test maps and checks all ten gates from the
design specification. `docs/course-concepts.md` maps each demonstrated feature
to Kafka, event time, Spark, HDFS, MapReduce, Parquet, SQL, data quality, and
scalability concepts. The documentation includes a 100-point self-assessment
rubric and a 5-to-7-minute live demonstration sequence.

- [ ] **Step 4: Run final local verification**

```text
ruff check src jobs dashboard tests scripts
ruff format --check src jobs dashboard tests scripts
mypy src dashboard
python -m pytest -m "not integration" -q
docker compose config --quiet
```

Expected: all commands exit successfully.

- [ ] **Step 5: Run the Docker fixture verification**

Run: `./scripts/run_fixture.ps1` on Windows or
`./scripts/run_fixture.sh` on Linux.

Expected: all layer counts reconcile and the final report is marked
`fixture-only`.

- [ ] **Step 6: Commission final independent reviews**

One reviewer checks specification compliance. A separate reviewer checks code
quality, distributed-system semantics, and forbidden claims. Resolve every
finding before release.

- [ ] **Step 7: Commit documentation and CI**

```text
git add README.md docs scripts .github
git commit -m "docs: finalize reproducible IoTBDM coursework workflow"
```

## Final Completion Gate

Do not run or publish the full experiment matrix until all fixture tests and
the Docker end-to-end workflow pass. Do not run full data on the development
machine. After fixture verification, execute the matrix on the designated
machine, copy back only finalized manifests and compact reports, and re-run the
independence scan before pushing results.

## Delivery Estimate

The P1 implementation budget is approximately 70 focused engineering hours,
including review and correction but excluding unattended full-data execution.
With existing general familiarity with Kafka, Spark, and Hadoop, this is seven
to ten calendar days. The implementation must not compress this estimate by
removing correctness gates. Brick enrichment remains deliberately narrow and
is the first item to defer if source metadata proves unexpectedly complex.

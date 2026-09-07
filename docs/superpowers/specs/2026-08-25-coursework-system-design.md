# IoT Energy Data Pipeline Coursework Design

## 1. Purpose

This repository implements an independent IoT and Big Data Management course
project over public smart-campus electricity telemetry. It demonstrates
reproducible ingestion, event-time processing, data-quality handling,
distributed storage, independent batch verification, SQL analytics, and a
small operational dashboard.

The project is coursework. It does not claim a new streaming algorithm, a new
energy model, or Internet-scale operation.

## 2. Independence Boundary

The repository must not contain or depend on intrusion-detection data, model
outputs, hardware measurements, unpublished research artifacts, research
names, or research-specific schemas. Its Git history starts from an empty
repository.

Only general engineering knowledge is reusable. Every domain contract,
dataset adapter, report, query, test fixture, and dashboard view is defined for
the public HKUST smart-meter dataset.

An automated repository scan must fail if forbidden prior-project names or
absolute local research paths occur in application code, user documentation,
queries, schemas, or reports. The scanner's own pattern configuration and
scanner tests are the only exclusions.

## 3. Dataset

The primary source is the HKUST multi-year campus-level smart-meter database
published in Scientific Data in 2024. The source contains raw and cleaned time
series, Brick metadata, more than 1,400 meters across more than 20 buildings,
and measurements collected from 2022 through May 2024.

The dataset is downloaded by the user from the official repository. Dataset
files are never committed. The project registers a local dataset root and
records file sizes, SHA-256 values, workbook structure, timestamp ranges,
numeric precision, sampling intervals, null rates, zero rates, duplicate
timestamp counts, and Brick join coverage.

No physical interpretation is inferred from a filename alone. The profiler
must establish whether a field represents power, interval energy, cumulative
energy, or an unknown measurement before an adapter assigns a unit or derives
an energy aggregate.

The cleaned source variant is used for the principal pipeline experiments. Raw
files are used only for source-quality comparison unless the profile proves a
safe common contract. Raw and cleaned records are never silently pooled.

Every dataset manifest declares exactly one `source_variant`. Raw and cleaned
variants produce separate profiles and canonical snapshot hashes. The
canonicalizer may trim surrounding whitespace and parse a documented timestamp
or numeric representation. It may not interpolate, impute, clip, smooth,
remove outliers, fill missing intervals, or reinterpret units.

## 4. Claims

Allowed claims are limited to measured behavior:

- at-least-once Kafka delivery with deterministic source and delivery identity
- bounded streaming deduplication under a declared watermark
- exact end-of-run deduplication over all Bronze deliveries
- independent Spark and Hadoop aggregate agreement under a fixed numeric contract
- measured throughput, elapsed time, storage size, and query time on the named machine
- observed handling of controlled duplicate, delayed, and malformed deliveries

The project must not claim universal exactly-once processing, live sensor
ingestion, natural network disorder, Internet scale, production readiness,
energy savings, causal operational improvement, or a novel algorithm.

## 5. Architecture

```text
Official HKUST files and Brick metadata
                  |
          Registration and profile
                  |
       Canonical Parquet and NDJSON
                  |
       Deterministic replay schedule
                  |
                Kafka
                  |
        Spark Bronze delivery archive
                  |
       +----------+-----------+
       |                      |
       v                      v
Bounded Silver          Parse quarantine
event-time view         and invalid records
       |
       v
Operational stream metrics

Bronze delivery archive
       |
       v
Exact batch parse and deduplication
       |
       v
Final Gold aggregates and SQL views

Canonical NDJSON -> HDFS -> Hadoop Streaming oracle
                                  |
Gold aggregates -----------------+-> field-level reconciliation
                                  |
                                  v
                         finalized report/dashboard
```

Spark Silver is an operational, watermark-bounded view. Gold is not produced
from Silver because Silver may exclude valid events that arrive after the
watermark. Gold is a deterministic batch pass over Bronze after replay
completion.

## 6. Identity Contracts

### 6.1 Source event identity

`source_event_id` identifies one source row, not one distinct measurement
value. It is the SHA-256 digest of:

```text
dataset_version | source_variant | source_relative_path |
workbook_sheet | source_row_number
```

This preserves two physically separate source rows even when meter, timestamp,
and value are identical. A separate `logical_measurement_key` identifies
same-meter same-time observations for quality analysis. It is never used as
source identity or canonical deduplication identity. Data-quality SQL reports
its multiplicity without deleting any source event.

### 6.2 Replay identity

`replay_run_id` is an explicit experiment identifier such as
`clean-1m-r02`. The run manifest binds it to the canonical snapshot hash,
schedule configuration hash, replay seed, requested event count, partition
count, and repetition number. A run identifier cannot be reused with a
different manifest.

### 6.3 Delivery identity

`delivery_id` identifies one Kafka delivery attempt. It is derived from the
replay run, schedule position, source event, and injection mode. Controlled
duplicates have distinct delivery identifiers and the same source event
identifier.

Malformed deliveries retain a valid Kafka key and delivery identifier even
when their value cannot be parsed. Bronze can therefore preserve and account
for them.

The replay schedule assigns every delivery to a partition with:

```text
unsigned_big_endian(SHA-256(source_event_id)[0:8]) mod partition_count
```

Duplicate deliveries for one source event therefore remain ordered within one
Kafka partition. No language runtime hash function is permitted.

## 7. Canonical Event Contract

The canonical source event contains:

- schema version
- dataset version and source variant
- source event identifier
- source relative path, workbook sheet, and row number
- meter identifier
- Brick entity identifier when matched
- all source-declared building and zone identifiers when available
- all metered entities and their explicit RDF types
- source Brick unit URI and HKUST usage type when available
- original timestamp text
- source timezone
- event time in UTC
- original numeric text
- signed 64-bit scaled value
- decimal scale
- verified measurement kind
- verified unit
- source-quality flags

The adapter uses `decimal.Decimal`. It converts source values into a signed
64-bit integer at the smallest documented common decimal scale that preserves
the source precision. It fails closed on overflow or unrepresentable values.
Spark and Hadoop group by unit and scale before summation.

The snapshot manifest includes a scale registry keyed by measurement kind and
unit. Each entry records the source evidence used for the assignment, decimal
scale, and allowed numeric representation. SHA-256 is used for source files,
manifests, and canonical snapshot artifacts.

Hong Kong timestamps use the declared `Asia/Hong_Kong` zone only after the
source profile confirms the timestamp convention. The original timestamp is
always retained. Ambiguous, nonexistent, or unparseable timestamps are flagged
or quarantined. They are never shifted to make them valid.

Zeros, repeated timestamps, and non-monotonic values are profiled but are not
automatically discarded. A record is rejected only by a documented source or
schema rule.

Brick enrichment is deliberately narrow. The registered TTL file and detected
Brick namespace are recorded by hash. A meter may match zero or one meter
entity. No match leaves the entity null and relationship collections empty but
remains valid. Multiple meter matches, malformed TTL, conflicting timeseries
identifiers, or contradictory scalar metadata fail canonicalization. Building,
zone, and metered-entity relationships remain sorted collections because the
deposited graph is legitimately multi-valued. No inferred ontology reasoning
is used; only explicit `hasPart` and `isMeteredBy` edges are projected.

## 8. Replay and Completion Contracts

Replay schedules are deterministic for a canonical snapshot, configuration,
and seed. Fault injection adds deliveries. It never removes the only valid
delivery of a source event.

The producer writes a run manifest before sending data. At production scale,
the deterministic delivery schedule is line-delimited NDJSON with a closed
metadata document and SHA-256 binding. The producer streams that schedule,
waits for Kafka acknowledgements, and appends each acknowledgement to an
NDJSON ledger. It emits one terminal control record per partition after that
partition's data, flushes the producer, and writes a small receipt containing
sent counts, terminal offsets, and the acknowledgement-ledger count, size, and
hash. Interrupted replay resumes only after validating the acknowledged prefix
against the immutable schedule. Compact fixture tests may use the equivalent
version-1 in-memory contract.

The Spark driver finalizes a run only when:

1. the producer receipt exists and matches the run manifest
2. Bronze contains every expected delivery identifier exactly once
3. every partition has reached its terminal offset
4. parse, quarantine, and Silver counts reconcile with Bronze

All run outputs are written below an immutable run directory. A fresh run
refuses to overwrite existing output. A restart uses the same run manifest and
Spark checkpoint. It may not change the schedule, source snapshot, or output
location.

Recovery equivalence is semantic. An interrupted-and-resumed run and an
uninterrupted control run must have identical sorted Gold records, oracle
records, reconciliation fields, and source coverage. Runtime identifiers,
elapsed times, Kafka offsets, and Parquet file bytes are not expected to match.

## 9. Event-Time and Quality Contracts

Silver applies a declared event-time watermark and bounded deduplication by
`source_event_id` within one replay run. The watermark duration must exceed the
maximum event-time lateness calculated from the complete replay schedule for
experiments intended to retain all valid source events in Silver. Configuration
validation fails before replay when this condition is not met.

Lateness is not a schema error. A parse-valid delayed event remains in Bronze.
Its final disposition is measured by comparing the injected schedule with
Silver and Gold after the run.

Quarantine categories are finite and machine-readable:

- invalid JSON
- schema mismatch
- unknown source event identifier
- invalid timestamp
- invalid numeric value
- numeric overflow
- missing required identity
- unit or scale contract violation
- run-manifest mismatch

Quarantine is retained as an immutable part of the run output. It participates
in layer-count reconciliation but not in energy aggregates.

## 10. Independent Oracle

The Hadoop Streaming oracle reads the canonical NDJSON snapshot from HDFS. It
does not import Spark transformation code and does not consume Spark Gold
records as its input.

Two independently implemented Hadoop Streaming jobs are used. The source-record
job emits a canonical projection of every valid source event, while the
aggregate job computes counts, scaled sums, minimum and maximum event times,
and quality-flag counts by declared grouping keys. Reconciliation compares both
sorted source records and sorted aggregate records field by field.

The source-record oracle validates exact finalized content and the aggregate
oracle independently validates grouped arithmetic and source coverage.
Producer receipts and Bronze delivery counts separately validate
Kafka-to-Bronze completeness. The report must state this boundary.

## 11. Experiments

The minimum matrix is:

| Family | Configurations | Repetitions |
| --- | --- | ---: |
| Correctness | clean, duplicates, delayed, malformed, combined | 1 |
| Scale | 100K, 1M, 5M or full if smaller | 3 |
| Storage | NDJSON, unpartitioned Parquet, partitioned compressed Parquet | each resolved scale |
| Query | meter, building, and time-window queries | 3 repetitions |
| Recovery | one interrupted and resumed fixture run | 1 |
| Watermark | retaining and deliberately restrictive fixture settings | 1 each |

Fault configurations use one fixed event count. Scale configurations use the
clean schedule. The third scale resolves to `min(5_000_000, full_count)` and is
omitted when it duplicates the 1M scale. Performance reporting uses median,
minimum, and maximum. The machine, container versions, partition count, replay
rate, and data hashes are recorded for every run.

Watermark fixtures use one partition and one offset per trigger. The forced
micro-batch sequence makes prior-watermark behavior deterministic; those
settings are semantic fixtures and are excluded from throughput claims.

## 12. Dashboard and Report

The dashboard is read-only and opens only finalized run manifests. It shows:

- run provenance and status
- throughput and layer counts
- injected-fault disposition
- energy and data-quality summaries
- storage and query measurements
- Spark-Hadoop reconciliation

The dashboard does not start jobs, edit configurations, or display unfinished
results.

## 13. Technology and Scope

The implementation uses Python 3.11 or newer, Kafka, PySpark Structured
Streaming, Hadoop Streaming, HDFS, Parquet, SQL, Streamlit, pytest, Ruff, and
Docker Compose.

Kubernetes, model training, forecasting, anomaly models, cloud deployment,
authentication, alerting, and a general-purpose orchestration framework are
outside the coursework scope.

## 14. Acceptance Gates

The coursework is ready for demonstration only when:

1. the source profile and canonical snapshot are reproducible from registered files
2. tracked files contain no forbidden prior-project references or source data
3. canonical event identities are unique and deterministic
4. clean and fault-injected fixture runs reconcile at every layer
5. Gold equals the Hadoop oracle for every exact source-record and aggregate field
6. malformed deliveries are preserved in Bronze and classified in quarantine
7. a resumed run has the same semantic finalized records as an uninterrupted run
8. the dashboard refuses unfinished or unreconciled runs
9. unit, property, contract, and container integration tests pass
10. a restrictive watermark changes the operational candidate while exact Gold and oracle records remain unchanged
11. every reported number is generated from a finalized run manifest

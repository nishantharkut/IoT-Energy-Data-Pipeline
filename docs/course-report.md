# Auditable IoT Energy Data Pipeline

## Coursework question

A successful Spark job proves that code reached its end; it does not by itself
prove that every source event arrived once, that every invalid delivery was
retained, or that the final aggregate is correct. This project implements an
auditable IoT energy pipeline whose evidence is independent of job completion.

## Public source and evidence boundary

The source is the public HKUST smart-meter deposit associated with Dryad DOI
`10.5061/dryad.k3j9kd5h6`. The source archive, Excel telemetry, and Brick RDF
are not committed. Raw and cleaned variants receive separate manifests,
profiles, and snapshots.

Bounded source inspection established the deposited file families and Brick
relationship shape. Full profiling and canonicalization remain deferred until
the reviewed rules registry has evidence for timezone, decimal scale, unit,
and measurement semantics. Filename conventions alone never establish these
claims.

## System design

The pipeline has four data responsibilities:

- Bronze preserves every acknowledged Kafka data delivery.
- Silver gives a watermark-bounded operational candidate and records duplicate
  or late dispositions.
- Quarantine preserves malformed and contract-violating deliveries by finite
  category.
- Gold is rebuilt from all Bronze deliveries after replay completion and uses
  exact structural source identity.

Canonical NDJSON independently enters HDFS. Two Hadoop Streaming jobs, whose
mapper and reducer code import no Spark transformations, reproduce sorted
source records and exact aggregates. Reconciliation compares every declared
field, verifies artifact hashes, and checks layer conservation.

## Exactness and provenance

A source event is one physical workbook row identified by dataset version,
source variant, relative path, sheet, and row number. Numeric values preserve
their original text and are represented as signed scaled integers. This avoids
binary floating-point disagreement between Spark and Hadoop.

Replay schedules use a fixed seed, deterministic partitioning, unique delivery
identifiers, and one terminal control record per partition. Fault injection may
add duplicates, delays, or malformed deliveries but may not remove the only
valid delivery of a source event.

## Decision impact

Interval consumption is derived only for verified cumulative-kWh contracts.
Negative deltas, ambiguous duplicate timestamps, unresolved resets, and
invalid readings are flagged instead of converted to valid consumption. The
report compares the Silver candidate with verified Gold and records affected
meter-periods, absolute kWh discrepancy, verification time, and explicitly
scoped storage overhead.

Rates 0.5, 1.0, and 2.0 are normalized hypothetical scenarios. No monetary
value is an HKUST tariff, bill, saving, or financial result.

## Experiment and publication controls

The fixed matrix covers five fault configurations, recovery, two watermarks,
three scale sizes, storage formats, and five read-only queries. Clean scale
runs have three repetitions and report median, minimum, and maximum.

Planning and execution are separate commands. The execution command requires a
literal confirmation phrase and designated-machine metadata. A dashboard run
is publishable only after exact reconciliation, decision-report validation,
query and storage benchmarks, and a self-contained SHA-256 seal.

## Current status

The implementation, contracts, lightweight tests, deferred orchestration,
dashboard loader, and report generator are present. The repository currently
makes no empirical full-data performance or financial claim. Such claims can
appear only in the generated results report after the designated-machine runs.

## Limitations

This is controlled replay, not live campus ingestion. It does not prove
universal exactly-once behavior, production readiness, energy savings, or a
novel algorithm. Kubernetes, forecasting, machine learning, cloud deployment,
adaptive verification, and real tariff analysis are outside scope.

# Reproducibility

Reproduction is an immutable sequence. A later stage refuses to overwrite an
earlier result and binds the exact hashes it consumed.

1. Verify the downloaded Dryad wrapper and its inner archive without extracting
   it implicitly.
2. Extract to an internal, reliable local path and register either `raw` or
   `clean` as one source variant.
3. Profile the registered files and audit them against
   `configs/hkust-v5-source-layout.json`.
4. Review a `canonicalization-rules-v1` document. Its timezone, unit,
   measurement kind, decimal scale, and evidence are explicit; its source
   layout file is hash-bound.
5. Build canonical NDJSON and Zstandard Parquet. NDJSON is the portable oracle
   input; Parquet is analytical storage.
6. Capture `machine-metadata-v1` and compile `experiment-plan-v1`. Compilation
   performs no workload execution.
7. On the designated machine only, use the explicitly gated executor. Every
   run receives a unique topic, schedule, receipt, terminal records, layer
   audit, Gold output, Hadoop outputs, reconciliation report, decision report,
   query benchmark, storage benchmark, and final seal.
8. Generate the course-results Markdown from `experiment-results-v1`. The
   generator reopens every sealed run and verifies its manifest hash.

Production-size replay artifacts use closed version-2 contracts. Canonical
selection is streamed, delivery ordering is externally sorted through a local
SQLite work file, and the deterministic schedule is written as NDJSON. Kafka
acknowledgements are appended to a separate NDJSON ledger; the small receipt
stores its count, byte size, and SHA-256 rather than embedding millions of
records. Resume accepts only a byte- and record-validated schedule prefix.
Fixture-only paths retain the in-memory version-1 representation for compact
tests, but do not define the production scale architecture.

Canonicalization, telemetry cadence audit, Spark-Hadoop reconciliation, and
decision-impact comparison also use bounded-memory or disk-backed processing.
No production path requires a Python list or dictionary containing the full
five-million-event workload.

The normal CI workflow runs only unit, property, and contract tests. Fixture
acceptance is a manual workflow. The 1M and larger workloads are never run live
during the course demonstration.

Absolute source paths are intentionally absent from persisted semantic
identities. Runtime data remains ignored by Git and must stay under the
repository runtime directory used by Docker Compose.

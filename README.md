# IoT Energy Data Pipeline

ABV-IIITM IoTBDM coursework using public HKUST smart-meter data.

## Overview

This coursework asks a narrow question: if a distributed data job finishes,
what evidence shows that its answer is complete and correct? The implementation
preserves every Kafka delivery in Bronze, exposes a watermark-bounded Silver
candidate, rebuilds exact Gold from Bronze after terminal offsets arrive, and
compares Gold with two independently implemented Hadoop Streaming outputs.

The repository is independent coursework and is not affiliated with HKUST.

## Source

Historical dataset files from public HKUST campus smart-meter data.

The downloaded Dryad wrapper can be verified without extraction using
`python -m iot_energy_pipeline.cli verify-package`. Registration, profiling,
source audit, and canonicalization are separate immutable stages. Raw and
cleaned variants are never silently combined, and measurement semantics must
come from a hash-bound evidence-rules file.

## Claims

No empirical result is claimed in the repository before a designated-machine
experiment produces sealed run manifests. Monetary values are normalized
hypothetical scenarios, never HKUST financial results.

## Architecture

```
HKUST Files -> Registration -> Parquet/NDJSON -> Kafka -> Spark Bronze -> Silver + Quarantine
                                         |                                    |
                                         v                                    v
                                   Hadoop Oracle                      Batch Gold
                                         |                                    |
                                         +----------> Reconciliation <--------+
                                                                              |
                                                                              v
                                                                           SQL -> Dashboard
```

Bronze preserves all deliveries. Silver is a bounded operational view. Gold
rebuilds from Bronze with exact source identity. Hadoop reads canonical NDJSON,
imports no Spark transformations, and emits both exact source records and
aggregates for field-level reconciliation.

## Implementation-only workflow

The following commands create contracts or plans; they do not start Kafka,
Spark, Hadoop, Docker, or experiment workloads.

```powershell
.venv\Scripts\python.exe -m iot_energy_pipeline.cli verify-package `
  --package <dryad-wrapper.zip> --output reports/package-verification.json

.venv\Scripts\python.exe -m iot_energy_pipeline.machine_metadata `
  --output reports/machine-metadata.json `
  --execution-context designated_machine `
  --python-version 3.11.9 --docker-version <version> `
  --kafka-version apache/kafka:3.7.1 `
  --spark-version 3.5.1 --hadoop-version 3.4.3

.\scripts\run_experiments.ps1 `
  -SnapshotManifest <snapshot-manifest.json> `
  -MachineMetadata reports/machine-metadata.json
```

Canonical snapshots are created only after an evidence registry has been
reviewed:

```powershell
.venv\Scripts\python.exe -m iot_energy_pipeline.cli canonicalize `
  --manifest <dataset-manifest.json> --root <registered-source-root> `
  --rules <canonicalization-rules.json> --output <snapshot-directory>
```

## Later designated-machine execution

Execution is intentionally separate and requires the exact confirmation phrase
shown below. Do not use this during implementation review.

```powershell
.\scripts\execute_experiments.ps1 `
  -ExperimentPlan reports/experiment-plan.json `
  -SnapshotDir <snapshot-directory> `
  -MachineMetadata reports/machine-metadata.json `
  -Confirm RUN-DESIGNATED-MACHINE-EXPERIMENTS
```

The executor uses an internal repository runtime directory, never deletes
Docker volumes, and publishes only runs that pass reconciliation, decision
report validation, query/storage benchmarks, and final integrity sealing.

## Verification during implementation

Lightweight checks do not execute the distributed fixture:

```powershell
.venv\Scripts\python.exe -m pytest tests/unit tests/property tests/contract -q
.venv\Scripts\python.exe -m ruff check src jobs dashboard tests scripts
.venv\Scripts\python.exe -m scripts.scan_independence
```

Fixture acceptance is manual-only in CI. Full scale workloads are never part of
a live demonstration. Production replay schedules and acknowledgements are
streamed NDJSON ledgers; the implementation does not retain a five-million-row
schedule, receipt, reconciliation, or decision comparison in Python memory.

## Structure

```
IoT-Energy-Data-Pipeline/
|-- src/iot_energy_pipeline/
|-- scripts/
|-- tests/
|-- configs/
|-- docs/
|-- jobs/spark/ and jobs/hadoop/
|-- queries/
|-- schemas/
```

## Documentation

- `docs/dataset-audit.md`: source evidence and bounded archive observations
- `docs/reproducibility.md`: immutable artifact sequence
- `docs/experiment-protocol.md`: fixed matrix and publication gates
- `docs/course-report.md`: coursework narrative before measured results
- `docs/demo-script.md`: 5–7 minute demonstration plan
- `docs/results/course-results.md`: generated-results publication boundary

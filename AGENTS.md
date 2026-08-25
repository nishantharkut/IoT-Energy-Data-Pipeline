# IoT Energy Data Pipeline - Agent Guidelines

## Project Identity

Coursework project for ABV-IIITM IoTBDM. Repository: `IoT-Energy-Data-Pipeline`.

## Data

Source data is not versioned. Local source data is processed but not committed.

## Development

### TDD
- Tests use tmp_path, never modify real Git index
- RED-GREEN cycle before production code

### Source event identification
- source_event_id is structural: dataset version, source variant, relative path, workbook sheet, physical row
- Never derive from measurement value
- Never use logical measurement key for canonical deduplication

### Units
- Never guess; derive from source documentation

## Prior research

No dependency on or copying of prior research code, data, results, or artifacts.

## Streaming contract

### Bronze
- Preserves all deliveries

### Silver
- Bounded operational deduplicated view
- Late valid records are not quarantine

### Gold
- Rebuilt from Bronze after replay completion
- Exact source_event_id deduplication
- Does not inherit Silver watermark drops

## Hadoop oracle

- Reads canonical NDJSON independently
- Must not import Spark

## Infrastructure

- Docker Compose allowed
- Kubernetes not used
- No full data run on this development system

## Version control

- Work on feature branches
- Implementation agents do not commit unless controller requests
- Verified controller batches may be committed
- Use git diff for verification
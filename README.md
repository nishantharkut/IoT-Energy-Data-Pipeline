# IoT Energy Data Pipeline

ABV-IIITM IoTBDM coursework using public HKUST smart-meter data.

## Overview

Coursework project demonstrating a data engineering pipeline. Not affiliated with HKUST.

## Source

Historical dataset files from public HKUST campus smart-meter data.

## Claims

No empirical results claimed. Full data runs on designated production machine.

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

Bronze preserves all deliveries. Silver is bounded operational view. Gold rebuilds from Bronze with exact dedup. Hadoop oracle reads NDJSON independently without Spark.

## Structure

```
IoT-Energy-Data-Pipeline/
|-- src/iot_energy_pipeline/
|-- scripts/
|-- tests/
|-- configs/
|-- docs/
```

## Development

See AGENTS.md.
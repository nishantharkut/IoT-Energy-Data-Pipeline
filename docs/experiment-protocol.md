# Experiment protocol

The matrix in `configs/experiments.toml` is fixed before execution.

| Family | Configuration | Repetitions |
| --- | --- | ---: |
| Correctness | clean, duplicate, delayed, malformed, combined fixture | 1 |
| Recovery | uninterrupted and interrupted-then-resumed fixture | 1 |
| Watermark | retaining and deliberately restrictive delayed fixture | 1 |
| Scale | 100K, 1M, `min(5M, full_count)` clean events | 3 |
| Storage | NDJSON, Zstandard Parquet, partitioned Zstandard Parquet | per run |
| Query | meter, building, time window, quality, exposure | 3 timings |

The final scale is omitted if it duplicates 1M. Scale summaries report median,
minimum, and maximum; a missing repetition fails result publication.

The two watermark fixtures deliberately use one Kafka partition and
`maxOffsetsPerTrigger=1`. This creates a deterministic global delivery order
across multiple Spark micro-batches so the previous watermark can classify the
injected delayed event. These fixture settings demonstrate semantics and are
not throughput measurements.

The compiler records a plan with status `planned_not_executed`. Execution needs
the literal confirmation `RUN-DESIGNATED-MACHINE-EXPERIMENTS`, matching
designated-machine metadata, and a snapshot whose hashes still match the plan.

Publication gates are fail-closed:

1. acknowledged deliveries equal Bronze deliveries;
2. terminal partition coverage is complete;
3. every Bronze delivery has one final disposition;
4. exact Gold covers every canonical source event once;
5. Spark and Hadoop source records and aggregates agree field by field;
6. the decision report is bound to the passed reconciliation;
7. query and storage benchmarks are complete for designated-machine runs;
8. all five fault schedules have identical semantic Gold and oracle records;
9. the restrictive watermark exposes an additional candidate difference while
   retaining identical exact Gold and oracle records;
10. the final run manifest and every registered artifact pass SHA-256 checks.

Each of the five query benchmarks must contain exactly three timings and the
declared median, minimum, and maximum. Storage descriptors must account for
NDJSON, unpartitioned Zstandard Parquet, and partitioned Zstandard Parquet,
including bytes, file counts, write time, relative size, and exact canonical
NDJSON hash. Incomplete or arithmetically inconsistent benchmark documents
cannot be sealed.

Recovery compares semantic Gold and oracle bytes; offsets, timing, and Parquet
layout bytes are not expected to match. Rates 0.5, 1.0, and 2.0 are normalized
hypothetical currency units per kWh, not HKUST tariffs or financial outcomes.

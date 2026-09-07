# 5–7 minute demonstration script

## Before the demonstration

- Use one already sealed fixture or 100K run.
- Keep 1M, 5M, and full-data results precomputed; never run them live.
- Open the run directory, reconciliation JSON, generated course report, and
  read-only dashboard before speaking.
- Do not start Docker services from the dashboard.

## 0:00–0:45 — The problem

“A Spark job finishing tells us that the program completed. It does not prove
that no IoT event was lost, duplicated, changed, or silently discarded. This
project makes every delivery auditable and checks the final result with an
independent engine.”

Show the architecture in the README. Point out the split between Kafka/Spark
and canonical NDJSON/Hadoop.

## 0:45–1:30 — Source and exact identity

Show the dataset manifest, source-layout evidence, and canonical schema. Explain
that a source event is a physical workbook row and that decimal text is stored
as a scaled integer. Mention that raw and cleaned variants are separate.

## 1:30–2:30 — Controlled replay and layers

Show one deterministic replay manifest and schedule. Explain unique delivery
IDs, fixed partitioning, injected duplicates/delays/malformed values, and
terminal partition records. In the dashboard, show Bronze, Silver, duplicate,
late, and quarantine counts conserving back to acknowledged deliveries.

## 2:30–3:35 — Exact Gold and independent Hadoop verification

Show that Gold is rebuilt from Bronze rather than Silver. Open the Hadoop
manifest and briefly show that the oracle consumes canonical NDJSON, not Spark
output. Display the reconciliation source-record and aggregate sections with
empty error lists.

## 3:35–4:35 — Why the watermark still matters

Compare retaining and restrictive Silver candidates. Explain that a restrictive
watermark may change the operational candidate while exact Gold remains
complete. Show affected meter-periods and flagged records; do not describe late
data as malformed data.

## 4:35–5:25 — Decision impact and guardrails

Show absolute kWh discrepancy and verification storage/time. State: “The 0.5,
1.0, and 2.0 rates are hypothetical normalized scenarios. They are not HKUST
tariffs or financial results.” Show residual scenario exposure after
verification as zero only because the published decision uses reconciled Gold.

## 5:25–6:15 — Scale, storage, queries, and recovery

Use the generated report, not a live workload. Show median/minimum/maximum over
three clean repetitions, NDJSON versus both Parquet layouts, and the five query
timings. Show that interrupted/resumed and uninterrupted recovery produced
identical semantic Gold and Hadoop records.

## 6:15–6:45 — Close

“The contribution is not another Spark pipeline. It is a reproducible evidence
chain showing where every delivery ended and whether an independently
implemented computation agrees. The limits are controlled replay, one declared
machine, and hypothetical business scenarios.”

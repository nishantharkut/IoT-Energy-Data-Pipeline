# HKUST Dataset Audit

## Status

The documentary source audit was completed on 2026-08-25. On 2026-08-26, the
downloaded DOI package was also inspected without running the data pipeline.
The package contains the official `All_Data.zip` and `README.md`; the inner
archive size and SHA-256 match Dryad version 5.

Only the ZIP directory, both Turtle members, and representative workbooks were
inspected. No all-workbook profile, canonical snapshot, Kafka replay, Spark
job, Hadoop job, or experiment was run. Global row counts, timestamp ranges,
and empirical quality results remain gated for the later data-run phase.

## Official Release

| Item | Verified value |
| --- | --- |
| Dataset | A 2.5-year campus-level smart meter database with equipment data for energy analytics |
| Dryad DOI | https://doi.org/10.5061/dryad.k3j9kd5h6 |
| Dryad version | 5 |
| License | CC0-1.0 |
| Data collection period | 2022-01-01 to 2024-05-27 |
| Total release size | 1,432,485,068 bytes |
| Main archive | `All_Data.zip`, 1,432,480,964 bytes |
| Archive SHA-256 | `f2446158573311bda2b1fbd8a114bb1479ac8d4a0e11c395ca3b920676eb0c8a` |
| Release README | `README.md`, 4,104 bytes |
| Data descriptor | https://doi.org/10.1038/s41597-024-04106-1 |
| Publisher correction | https://doi.org/10.1038/s41597-025-04435-9 |
| Authors' code | https://github.com/LiMingchen159/HKUST_Meter_Brick |

The local DOI wrapper is 1,432,485,316 bytes. This wrapper size includes ZIP
container overhead and must not be confused with the 1,432,485,068-byte sum of
the two official Dryad members.

## Bounded Archive Inspection

The central directory contains 2,725 files: 2,723 workbooks and two copies of
the Brick metadata (one under each source variant).

| Archive subtree | Workbook count |
| --- | ---: |
| `All Data/Raw Dataset/Time-series data` | 1,394 |
| `All Data/Clean Dataset/Resappled data/T15` | 403 |
| `All Data/Clean Dataset/Resappled data/T30` | 645 |
| `All Data/Clean Dataset/Resappled data/T60` | 255 |
| `All Data/Clean Dataset/Resappled data/T1440` | 26 |

`Resappled data` is the spelling in the deposited archive and is retained as
source provenance. Representative files from every subtree have one worksheet
named `Sheet1`, the exact columns `time` and `number`, and names of the form
`GUI_NO.<meter-id>.xlsx`. The implementation treats this as a closed layout;
unobserved fallback names are rejected.

The raw examples contain naive Excel datetimes and may have seconds not aligned
to cadence boundaries. The clean examples are aligned to their declared
cadence. This is structural evidence only, not a global timestamp profile or a
timezone assignment.

The registered Turtle graph has 72,091 triples and 1,432 external timeseries
references. HKUST links each reference with
`ref:hasTimeseriesData "<meter-id>.xlsx"`; it does not use
`ref:hasTimeseriesId`. Every inspected meter reference has one owning meter,
one zone, and the Brick unit URI `unit:KiloW-HR`. Building membership is
multi-valued for 424 meters, and metered-entity membership is multi-valued for
217 meters. The canonical contract therefore preserves sorted collections
rather than selecting an arbitrary building or equipment.

The Dryad API currently identifies version 5 as version record `308602`. The
publisher correction concerns extraneous commas in Equation 1. It does not
report a change to the deposited data files.

## Reported Structure

The Dryad README reports two source variants.

- The raw variant contains 1,394 `.xlsx` time-series files and one Brick
  metadata file named `HKUST_Meter_Metadata.ttl`.
- The clean variant contains resampled workbooks grouped by cadence. The
  reported counts are 403 files at 15 minutes, 645 at 30 minutes, 255 at one
  hour, and 26 at one day. These counts total 1,329 files.
- The release documentation says that 65 files were excluded from the clean
  data because of missing data or all-zero values.
- The article describes Excel time-series files and Turtle metadata. It
  reports approximately 773 MB of raw data, 830 MB after resampling, and
  approximately 1.43 GB compressed.

The public description covers more than 1,400 meters across more than 20
buildings. The article gives several related counts that must not be treated
as interchangeable: 1,412 reported meters, 1,394 raw workbooks, 65 failed
APIs, 46 all-zero meters, and 1,283 meters considered reliable for analysis.
The deposited Turtle graph exposes 1,432 external references. The later full
audit must reconcile these populations rather than select one count as the
universal meter total.

## Known Data-Quality Conditions

The article and release documentation establish the following conditions.

- Native sampling intervals vary between 15 minutes, 30 minutes, one hour,
  and one day.
- Missing values are represented by `NAN`. The raw data was not imputed and
  was not passed through general outlier removal.
- Reported quarterly missing rates are substantial at finer cadences. The
  article reports ranges of 23.467 to 52.417 percent at 15 minutes, 18.548 to
  36.680 percent at 30 minutes, 13.988 to 29.656 percent at one hour, and
  3.377 to 13.584 percent at one day.
- The authors' preprocessing code detects duplicate timestamps and keeps the
  first occurrence before resampling.
- That code infers cadence from the most common gap among the final 100
  timestamps, falls back to one hour when no supported cadence matches, drops
  files with fewer than 100 rows, and resamples numeric values with the mean.

These transformations mean that raw and clean files have different evidence
roles. They must never be pooled silently. The clean variant is suitable for
the principal coursework pipeline only when its manifest, cadence category,
and transformation boundary remain explicit. Raw files are retained for the
source-quality comparison.

## Measurement Semantics

The evidence for cumulative-register semantics is a combination of two
publisher-controlled sources; it is not inferred merely from the column name
`number`:

- The article describes device resolution and maximum *readings* in kWh. For
  example, the SKYDER meter has 0.1 kWh resolution and a maximum reading of
  99,999,999.9 kWh.
- In the authors' repository at the audited commit,
  `Evaluation/Data_Calculation.py` takes the first reading per day and applies
  `diff()` to obtain daily `total_kwh` and `sub_meter_kwh`. The dormitory
  analysis likewise labels the summed register `All_kWh` and differences
  consecutive hourly values.
- The deposited Brick graph supplies `unit:KiloW-HR` per matched meter. That
  metadata is the per-meter unit gate; a repository-wide global unit is still
  prohibited.

Together these sources support a reviewed `cumulative_energy`/`kWh` rule for
an individually matched meter. They do not authorize a blanket rule for an
unmatched workbook. Therefore:

1. A sum of raw meter readings is not consumed energy.
2. Spark and Hadoop may compare a scaled-value sum as an exact numeric
   reconciliation field, but the report must label it as an integrity checksum.
3. Energy consumption may be derived only after the adapter verifies the meter
   unit from Brick metadata and binds the cumulative-reading rule to the
   documentary evidence above.
4. The decision-impact implementation flags negative deltas or unresolved
   resets, ambiguous duplicate timestamps, and the first reading in each
   unambiguous series instead of treating them as consumption. Gaps remain
   explicit intervals between retained readings; no value is imputed.
5. The real-data decision report remains disabled until a reviewed timezone
   contract and per-meter evidence registry are supplied. The implementation
   does not report energy saved, and scenario exposure is never presented as
   an HKUST financial result.

Meter specifications in the article use kWh resolutions of 0.1 or 0.01 for
the named devices. This does not justify assigning one global resolution or
unit to every file. Unit and scale remain `UNKNOWN` until the registered Brick
metadata and observed exact numeric representation support an assignment for
the specific meter.

The clean workbooks are transformed register series: the authors' preprocessing
code removes duplicate timestamps by keeping the first row and then resamples
with the arithmetic mean. A clean value must therefore retain its
`source_variant=clean` provenance and must not be described as an unmodified
physical register observation. Raw and clean variants are never pooled.

The source documentation does not provide a timezone contract for the workbook
timestamps. Geographic location and naive Excel datetime values are not enough
to alter timestamp values, and profiling cannot prove a timezone. The original
timestamp text must be retained. UTC conversion remains disabled for the real
dataset until a publisher or author declaration establishes the convention.

## Content-Level Audit Gate

The designated data-run machine must complete these checks before
canonicalization:

1. Verify the downloaded archive size and SHA-256 against Dryad version 5.
2. Record the extracted top-level layout without renaming source files.
3. Count `.xlsx` and `.ttl` files separately for raw and clean variants.
4. Parse every workbook in read-only mode and record sheet names, headers, row
   counts, column counts, and corrupt or empty files.
5. Profile timestamp representation, parse failures, ordering, duplicate
   timestamps, observed range, and cadence distribution.
6. Profile numeric representation, `NAN`, blank, zero, negative, non-finite,
   reset, rollover, and non-monotonic conditions without changing values.
7. Parse the Brick graph and report meter-to-file, meter-to-building, and
   meter-to-equipment join coverage, including missing and multiple matches.
8. Compare raw and clean file membership and explain every exclusion or
   unmatched file.
9. Produce byte-identical manifests and profiles on two repeated executions.
10. Approve unit, scale, timezone, and cumulative-reading rules only when each
    rule points to source evidence.

## Fitness for the Coursework

The dataset is a good fit for IoT and Big Data Management because it combines
multi-year telemetry, many meter sources, several cadences, known missingness,
duplicate-timestamp handling, Excel source files, and Brick metadata. These
properties support streaming replay, event-time processing, distributed
storage, data-quality reporting, metadata joins, SQL analysis, and independent
batch reconciliation.

This fitness statement concerns the dataset's structure and documented
conditions. Performance and scalability claims will come only from finalized
pipeline runs with saved manifests.

## Evidence Sources

- Dryad dataset record: https://datadryad.org/dataset/doi:10.5061/dryad.k3j9kd5h6
- Dryad API metadata: https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.k3j9kd5h6
- Dryad version 5 files: https://datadryad.org/api/v2/versions/308602/files
- Scientific Data article: https://www.nature.com/articles/s41597-024-04106-1
- Publisher correction: https://www.nature.com/articles/s41597-025-04435-9
- Authors' repository, audited at commit `73c619ae33fdba6ca40f70905cd505bd554b1918`:
  https://github.com/LiMingchen159/HKUST_Meter_Brick
- Authors' daily register differencing implementation:
  https://github.com/LiMingchen159/HKUST_Meter_Brick/blob/73c619ae33fdba6ca40f70905cd505bd554b1918/Evaluation/Data_Calculation.py
- Authors' resampling implementation:
  https://github.com/LiMingchen159/HKUST_Meter_Brick/blob/73c619ae33fdba6ca40f70905cd505bd554b1918/Data%20Preprocessing/Data%20Resampling.py

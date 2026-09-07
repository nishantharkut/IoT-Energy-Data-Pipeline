# HKUST Smart Meter Dataset

## Data Source

The dataset used in this coursework is the publicly available HKUST campus-level smart meter database published in Scientific Data.

### Publication

- Scientific Data article: https://doi.org/10.1038/s41597-024-04106-1
- Publisher correction: https://doi.org/10.1038/s41597-025-04435-9

### Dataset Repository

- Official Dryad repository: https://doi.org/10.5061/dryad.k3j9kd5h6
- Related GitHub repository: https://github.com/LiMingchen159/HKUST_Meter_Brick

### Dryad Version 5 Metadata

- Title: A 2.5-year campus-level smart meter database with equipment data for energy analytics
- Total size: 1,432,485,068 bytes
- Files: `All_Data.zip` and `README.md`
- `All_Data.zip` SHA-256: `f2446158573311bda2b1fbd8a114bb1479ac8d4a0e11c395ca3b920676eb0c8a`

## Download

The full archive is not bundled in this repository. It can be downloaded from the official Dryad record at https://doi.org/10.5061/dryad.k3j9kd5h6.

Verify a downloaded DOI wrapper before extraction:

```bash
python -m iot_energy_pipeline.cli verify-package \
  --package doi_10_5061_dryad_k3j9kd5h6__v20240801.zip \
  --output manifests/hkust-v5-package-verification.json
```

This streams and hashes the two package members. It does not extract or process
the telemetry. The verification report contains no machine-specific absolute
path.

## Local Source Layout

After downloading and extracting the dataset, place the files in a local directory structure:

```
data/
  raw/
    hkust/
      v5/
        # Extracted xlsx and ttl files
```

Or for a processed variant:

```
data/
  clean/
    hkust/
      v5/
        # Processed files
```

Dryad version 5 contains `All Data/Raw Dataset` and
`All Data/Clean Dataset/Resappled data`. The misspelling `Resappled data` is
part of the deposited source path and must not be silently renamed. Register
raw and clean roots separately.

## Registration

Register a dataset variant using the CLI:

```bash
python -m iot_energy_pipeline.cli register \
  --root data/raw/hkust/v5 \
  --output manifests/hkust-v5-raw.json \
  --dataset-version v5 \
  --source-variant raw
```

For a processed variant:

```bash
python -m iot_energy_pipeline.cli register \
  --root data/clean/hkust/v5 \
  --output manifests/hkust-v5-clean.json \
  --dataset-version v5 \
  --source-variant clean
```

## Profiling

Profile a registered dataset:

```bash
python -m iot_energy_pipeline.cli profile \
  --manifest manifests/hkust-v5-raw.json \
  --root data/raw/hkust/v5 \
  --output profiles/hkust-v5-raw-profile.json
```

## Git Exclusion

Source data directories are excluded from version control via `.gitignore`. Do not commit dataset files to the repository.

## Units and Measurement Semantics

Units, decimal scale, measurement kind, and timezone are separate evidence
decisions. A meter may receive `kWh` and `cumulative_energy` only when its
registered Brick record supplies `unit:KiloW-HR` and the reviewed rule cites
the article plus the authors' register-differencing implementation. Unmatched
meters remain `UNKNOWN`.

No real-dataset timezone is currently assigned. The documentation identifies
the Hong Kong location but does not declare the convention of the naive Excel
timestamps. Do not create a real canonical snapshot until that source-timezone
evidence is supplied. Synthetic fixtures use an explicit timezone solely as a
test contract.

See `docs/dataset-audit.md` for the exact source boundary. Do not guess units,
scale, semantics, or timezone from geography or filenames.

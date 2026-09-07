#!/usr/bin/env sh
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 SNAPSHOT_MANIFEST MACHINE_METADATA OUTPUT" >&2
  exit 2
fi

pipeline_python="${PIPELINE_PYTHON:-python}"
"$pipeline_python" -m iot_energy_pipeline.experiment_plan \
  --matrix configs/experiments.toml \
  --snapshot-manifest "$1" \
  --machine-metadata "$2" \
  --output "$3"

#!/usr/bin/env sh
set -eu

if [ "$#" -ne 5 ]; then
  echo "usage: $0 PLAN SNAPSHOT_DIR MACHINE_METADATA RUNTIME_ROOT CONFIRM" >&2
  exit 2
fi
if [ "$5" != "RUN-DESIGNATED-MACHINE-EXPERIMENTS" ]; then
  echo "explicit execution confirmation is required" >&2
  exit 2
fi

pipeline_python="${PIPELINE_PYTHON:-python}"
"$pipeline_python" -m iot_energy_pipeline.experiment_execution \
  --experiment-plan "$1" \
  --snapshot-dir "$2" \
  --machine-metadata "$3" \
  --runtime-root "$4" \
  --repository "$(pwd)" \
  --python "$pipeline_python" \
  --confirm "$5"

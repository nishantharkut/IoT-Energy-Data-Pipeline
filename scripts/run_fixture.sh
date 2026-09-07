#!/usr/bin/env sh
set -eu

output="${1:-reports/runtime/fixture-combined}"
fault="${2:-combined}"
watermark_seconds="${3:-60}"

python -m iot_energy_pipeline.fixture \
  --output "$output" \
  --run-id "fixture-$fault" \
  --fault "$fault" \
  --watermark-seconds "$watermark_seconds"
python -m pytest \
  tests/integration/test_fixture_acceptance.py \
  tests/integration/test_hadoop_streaming.py -q

param(
    [string]$Output = 'reports/runtime/fixture-combined',
    [ValidateSet('clean', 'duplicate', 'delayed', 'malformed', 'combined')]
    [string]$Fault = 'combined',
    [int]$WatermarkSeconds = 60,
    [int]$InterruptAfter = 0
)

$ErrorActionPreference = 'Stop'
$arguments = @(
    '-m', 'iot_energy_pipeline.fixture',
    '--output', $Output,
    '--run-id', "fixture-$Fault",
    '--fault', $Fault,
    '--watermark-seconds', $WatermarkSeconds
)
if ($InterruptAfter -gt 0) {
    $arguments += @('--interrupt-after', $InterruptAfter)
}
& .venv\Scripts\python.exe @arguments
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& .venv\Scripts\python.exe -m pytest `
    tests/integration/test_fixture_acceptance.py `
    tests/integration/test_hadoop_streaming.py -q
exit $LASTEXITCODE

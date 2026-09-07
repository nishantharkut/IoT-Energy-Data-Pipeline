param(
    [string]$Matrix = 'configs/experiments.toml',
    [Parameter(Mandatory = $true)]
    [string]$SnapshotManifest,
    [Parameter(Mandatory = $true)]
    [string]$MachineMetadata,
    [string]$Output = 'reports/experiment-plan.json'
)

$ErrorActionPreference = 'Stop'
$arguments = @(
    '-m', 'iot_energy_pipeline.experiment_plan',
    '--matrix', $Matrix,
    '--snapshot-manifest', $SnapshotManifest,
    '--machine-metadata', $MachineMetadata,
    '--output', $Output
)
& .venv\Scripts\python.exe @arguments
exit $LASTEXITCODE

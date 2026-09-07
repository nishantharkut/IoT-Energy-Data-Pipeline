param(
    [Parameter(Mandatory = $true)]
    [string]$ExperimentPlan,
    [Parameter(Mandatory = $true)]
    [string]$SnapshotDir,
    [Parameter(Mandatory = $true)]
    [string]$MachineMetadata,
    [string]$RuntimeRoot = 'reports/runtime/experiments',
    [Parameter(Mandatory = $true)]
    [ValidateSet('RUN-DESIGNATED-MACHINE-EXPERIMENTS')]
    [string]$Confirm
)

$ErrorActionPreference = 'Stop'
& .venv\Scripts\python.exe -m iot_energy_pipeline.experiment_execution `
    --experiment-plan $ExperimentPlan `
    --snapshot-dir $SnapshotDir `
    --machine-metadata $MachineMetadata `
    --runtime-root $RuntimeRoot `
    --repository (Get-Location).Path `
    --python (Resolve-Path '.venv\Scripts\python.exe').Path `
    --confirm $Confirm
exit $LASTEXITCODE

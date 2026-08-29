[CmdletBinding()]
param(
    [string]$Context = 'kind-edgeroute',
    [string]$SourceImage = 'edgeroute-coredns:dev'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$demoRoot = Join-Path $repoRoot ('.tmp\demo-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))

Write-Host '0:00-0:45  Architecture: EdgeCDN-X geographic routing + EdgeRoute quality control + HLS testbed'
Write-Host '              See docs/architecture.md for the Mermaid diagram and trust boundaries.'
Write-Host '0:45-1:30  Normal state and three real edge-cache checks'
kubectl --context $Context -n edge-system get nodequalities.adaptive.edgecdnx.io
if ($LASTEXITCODE -ne 0) { throw 'Unable to read NodeQuality state.' }

Write-Host '1:30-4:15  Run an automated adaptive latency-fault smoke test and recovery'
& (Join-Path $PSScriptRoot 'e2e-smoke.ps1') `
    -Context $Context `
    -SourceImage $SourceImage `
    -OutputRoot $demoRoot

$runDirectory = @(Get-ChildItem -LiteralPath $demoRoot -Directory)[0]
$events = Get-Content -LiteralPath (Join-Path $runDirectory.FullName 'nodequality-events.jsonl') |
    ForEach-Object { $_ | ConvertFrom-Json }

Write-Host 'Captured NodeQuality states:'
foreach ($event in $events) {
    $states = @($event.payload.items | ForEach-Object {
        '{0}={1}/{2}' -f $_.metadata.name, $_.status.state, $_.status.effectiveWeight
    }) -join ', '
    Write-Host ('  {0}: {1}' -f $event.stage, $states)
}

Write-Host '4:15-5:00  Recorded comparison and limits'
Write-Host '              experiments/results/processed/policy-comparison.png'
Write-Host '              smoke profile = pipeline/fault evidence, not a production performance claim.'
Write-Host ('Temporary demo evidence: {0}' -f $runDirectory.FullName)

[CmdletBinding()]
param(
    [string]$Context = 'kind-edgeroute',
    [string]$SourceImage = 'edgeroute-coredns:dev',
    [string]$OutputRoot = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runRoot = if ($OutputRoot) {
    if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
        [System.IO.Path]::GetFullPath($OutputRoot)
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputRoot))
    }
} else {
    Join-Path $repoRoot ('.tmp\e2e-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ'))
}

function Invoke-Checked {
    param([Parameter(Mandatory)][scriptblock]$Command, [Parameter(Mandatory)][string]$Description)
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$Description failed with exit code $LASTEXITCODE." }
}

Push-Location -LiteralPath $repoRoot
try {
    Invoke-Checked -Description 'CoreDNS rollout check' -Command {
        kubectl --context $Context -n edge-system rollout status deployment/edgeroute-coredns --timeout=60s
    }
    Invoke-Checked -Description 'Quality Controller rollout check' -Command {
        kubectl --context $Context -n edge-system rollout status deployment/quality-controller --timeout=60s
    }
    & (Join-Path $PSScriptRoot 'verify-hls.ps1') -Context $Context

    & (Join-Path $repoRoot 'experiments\run-day6.ps1') `
        -Profile smoke `
        -Variants adaptive `
        -Scenarios latency `
        -Repetitions 1 `
        -InjectionDelaySeconds 4 `
        -Context $Context `
        -SourceImage $SourceImage `
        -OutputRoot $runRoot

    $runDirectories = @(Get-ChildItem -LiteralPath $runRoot -Directory)
    if ($runDirectories.Count -ne 1) {
        throw "Expected one e2e run under $runRoot; found $($runDirectories.Count)."
    }
    $runDirectory = $runDirectories[0]
    $metadata = Get-Content -Raw -LiteralPath (Join-Path $runDirectory.FullName 'experiment-metadata.json') | ConvertFrom-Json
    $summary = Get-Content -Raw -LiteralPath (Join-Path $runDirectory.FullName 'k6-summary.json') | ConvertFrom-Json
    $prometheus = Get-Content -Raw -LiteralPath (Join-Path $runDirectory.FullName 'prometheus-range-query.json') | ConvertFrom-Json
    $failureRate = [double]$summary.metrics.hls_session_failures.value

    if ($metadata.k6_exit_code -ne 0) { throw "k6 exit code was $($metadata.k6_exit_code)." }
    if ($failureRate -gt 0.05) { throw "HLS session failure rate $failureRate exceeded the 5% e2e ceiling." }
    if ($prometheus.status -ne 'success' -or @($prometheus.data.result).Count -eq 0) {
        throw 'Prometheus did not return non-empty cache/response telemetry.'
    }

    $corefileOutput = kubectl --context $Context -n edge-system get configmap/edgeroute-coredns -o jsonpath='{.data.Corefile}'
    if ($LASTEXITCODE -ne 0) { throw 'Unable to read the restored Corefile.' }
    $corefile = $corefileOutput -join "`n"
    if ($corefile -notmatch 'routingmode\s+adaptive') {
        throw 'The e2e runner did not restore adaptive routing mode.'
    }

    [pscustomobject]@{
        Result = 'PASS'
        RunID = $metadata.run_id
        JobUID = $metadata.job_uid
        K6Pod = $metadata.k6_pod
        SessionFailureRate = $failureRate
        PrometheusSeries = @($prometheus.data.result).Count
        Evidence = $runDirectory.FullName
    } | Format-List
} finally {
    Pop-Location
}

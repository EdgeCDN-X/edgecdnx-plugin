[CmdletBinding()]
param(
    [ValidateSet('smoke', 'full')][string]$Profile = 'smoke',
    [ValidateSet('baseline', 'static-rendezvous', 'adaptive')][string[]]$Variants = @('baseline', 'static-rendezvous', 'adaptive'),
    [ValidateSet('latency', 'disconnect', 'pod-down', 'cold-cache')][string[]]$Scenarios = @('latency', 'disconnect', 'pod-down', 'cold-cache'),
    [ValidateRange(1, 20)][int]$Repetitions = 3,
    [ValidateRange(1, 120)][int]$InjectionDelaySeconds = 4,
    [string]$Context = 'kind-edgeroute',
    [string]$SourceImage = 'edgeroute-coredns:dev',
    [string]$K6Image = 'grafana/k6:1.8.0',
    [string]$K6RuntimeImage = 'edgeroute-k6:1.8.0',
    [string]$OutputRoot = ''
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'lib\Day6.Common.ps1')

$namespace = 'edge-system'
$rawRoot = if ($OutputRoot) {
    if ([System.IO.Path]::IsPathRooted($OutputRoot)) {
        [System.IO.Path]::GetFullPath($OutputRoot)
    } else {
        [System.IO.Path]::GetFullPath((Join-Path $repoRoot $OutputRoot))
    }
} else {
    Join-Path $PSScriptRoot 'results\raw'
}
$shortCommit = (& git -C $repoRoot rev-parse --short HEAD).Trim()
$fullCommit = (& git -C $repoRoot rev-parse HEAD).Trim()
if (-not $shortCommit -or $LASTEXITCODE -ne 0) { throw 'Unable to resolve the EdgeRoute Git commit.' }

function Invoke-Day6Kubectl {
    param([Parameter(Mandatory)][string[]]$Arguments)
    Invoke-CheckedKubectl -Arguments (@('--context', $Context) + $Arguments)
}

function Save-Text {
    param([Parameter(Mandatory)][string]$Path, [AllowEmptyString()][string]$Value)
    $parent = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force $parent | Out-Null
    [System.IO.File]::WriteAllText($Path, $Value, [System.Text.UTF8Encoding]::new($false))
}

function Save-NodeQualitySnapshot {
    param([Parameter(Mandatory)][string]$RunDirectory, [Parameter(Mandatory)][string]$Stage)
    $raw = (Invoke-Day6Kubectl -Arguments @('get', 'nodequalities', '-n', $namespace, '-o', 'json')) -join "`n"
    $entry = [ordered]@{
        capturedAt = (Get-Date).ToUniversalTime().ToString('o')
        stage = $Stage
        payload = $raw | ConvertFrom-Json
    } | ConvertTo-Json -Depth 30 -Compress
    [System.IO.File]::AppendAllText((Join-Path $RunDirectory 'nodequality-events.jsonl'), $entry + "`n", [System.Text.UTF8Encoding]::new($false))
}

function Reset-NodeQualityBaseline {
    Invoke-Day6Kubectl -Arguments @('scale', 'deployment/quality-controller', '-n', $namespace, '--replicas=0') | Out-Null
    Invoke-Day6Kubectl -Arguments @('rollout', 'status', 'deployment/quality-controller', '-n', $namespace, '--timeout=90s') | Out-Null
    $now = (Get-Date).ToUniversalTime().ToString('o')
    foreach ($node in @('edge-syd-a', 'edge-syd-b', 'edge-sin-a')) {
        $status = [ordered]@{
            state = 'Healthy'
            reason = 'Day 6 controlled experiment reset'
            effectiveWeight = 100
            qualityScore = 1.0
            ejectionCount = 0
            consecutiveFailures = 0
            consecutiveHealthy = 0
            recoveryStep = 0
            observedAt = $now
            stateSince = $now
        }
        $payload = @{status = $status} | ConvertTo-Json -Depth 8 -Compress
        Invoke-Day6Kubectl -Arguments @('patch', 'nodequality', $node, '-n', $namespace, '--subresource=status', '--type=merge', '-p', $payload) | Out-Null
    }
}

function Set-Variant {
    param([Parameter(Mandatory)][ValidateSet('baseline', 'static-rendezvous', 'adaptive')][string]$Variant)
    $routingMode = switch ($Variant) {
        'baseline' { 'deterministic' }
        'static-rendezvous' { 'static-rendezvous' }
        default { 'adaptive' }
    }
    $tag = "edgeroute-coredns:$Variant-$shortCommit"
    & docker image inspect $SourceImage *> $null
    if ($LASTEXITCODE -ne 0) { throw "Required source image '$SourceImage' was not found." }
    & docker tag $SourceImage $tag
    if ($LASTEXITCODE -ne 0) { throw "Unable to tag $tag." }
    & (Join-Path $repoRoot '.tools\kind.exe') load docker-image $tag --name edgeroute | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Unable to load $tag into kind." }

    $corefile = (Invoke-Day6Kubectl -Arguments @('get', 'configmap/edgeroute-coredns', '-n', $namespace, '-o', 'jsonpath={.data.Corefile}')) -join "`n"
    if ($corefile -match 'routingmode\s+(adaptive|static-rendezvous|deterministic)') {
        $corefile = [regex]::Replace($corefile, 'routingmode\s+(adaptive|static-rendezvous|deterministic)', "routingmode $routingMode")
    } elseif ($corefile -match '(?m)^(\s*defaultweight\s+\d+\s*)$') {
        $corefile = [regex]::Replace($corefile, '(?m)^(\s*defaultweight\s+\d+\s*)$', "`$1`n        routingmode $routingMode")
    } else {
        throw 'Corefile has neither routingmode nor defaultweight directive.'
    }
    $patch = @{data = @{Corefile = $corefile}} | ConvertTo-Json -Depth 5 -Compress
    Invoke-Day6Kubectl -Arguments @('patch', 'configmap/edgeroute-coredns', '-n', $namespace, '--type=merge', '-p', $patch) | Out-Null
    Invoke-Day6Kubectl -Arguments @('set', 'image', 'deployment/edgeroute-coredns', '-n', $namespace, "coredns=$tag") | Out-Null
    Invoke-Day6Kubectl -Arguments @('rollout', 'restart', 'deployment/edgeroute-coredns', '-n', $namespace) | Out-Null
    Invoke-Day6Kubectl -Arguments @('rollout', 'status', 'deployment/edgeroute-coredns', '-n', $namespace, '--timeout=180s') | Out-Null

    Reset-NodeQualityBaseline
    if ($Variant -eq 'adaptive') {
        Invoke-Day6Kubectl -Arguments @('scale', 'deployment/quality-controller', '-n', $namespace, '--replicas=1') | Out-Null
        Invoke-Day6Kubectl -Arguments @('rollout', 'status', 'deployment/quality-controller', '-n', $namespace, '--timeout=120s') | Out-Null
    }
    return $tag
}

function Initialize-K6Resources {
    & docker image inspect $K6Image *> $null
    if ($LASTEXITCODE -ne 0) {
        & docker pull $K6Image | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "Unable to pull $K6Image." }
    }
    & docker build --platform linux/amd64 --build-arg "K6_IMAGE=$K6Image" --tag $K6RuntimeImage --file (Join-Path $PSScriptRoot 'k6\Dockerfile') (Join-Path $PSScriptRoot 'k6') | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Unable to build single-platform runtime $K6RuntimeImage." }
    & (Join-Path $repoRoot '.tools\kind.exe') load docker-image $K6RuntimeImage --name edgeroute | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Unable to load $K6RuntimeImage into kind." }

    $scriptPath = Join-Path $PSScriptRoot 'k6\hls.js'
    $yaml = & kubectl --context $Context create configmap edgeroute-k6-script -n $namespace "--from-file=hls.js=$scriptPath" --dry-run=client -o yaml
    if ($LASTEXITCODE -ne 0) { throw 'Unable to render k6 script ConfigMap.' }
    $yaml | & kubectl --context $Context apply -f - | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Unable to apply k6 script ConfigMap.' }
    Invoke-Day6Kubectl -Arguments @('apply', '-f', (Join-Path $PSScriptRoot 'k6\prefixlist.yaml')) | Out-Null
}

function Install-K6Job {
    $dnsServiceIP = ((Invoke-Day6Kubectl -Arguments @(
        'get', 'service/edgeroute-coredns', '-n', $namespace,
        '-o', 'jsonpath={.spec.clusterIP}'
    )) -join '').Trim()
    $parsedAddress = $null
    if (-not [System.Net.IPAddress]::TryParse($dnsServiceIP, [ref]$parsedAddress)) {
        throw "EdgeRoute DNS Service returned invalid ClusterIP '$dnsServiceIP'."
    }

    $jobTemplate = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'k6\job.yaml')
    if (-not $jobTemplate.Contains('__EDGEROUTE_DNS_SERVICE_IP__')) {
        throw 'k6 Job template does not contain the DNS Service IP placeholder.'
    }
    $renderedJob = $jobTemplate.Replace('__EDGEROUTE_DNS_SERVICE_IP__', $dnsServiceIP)
    $renderedJob | & kubectl --context $Context apply -f - | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Unable to apply the rendered k6 Job.' }
}

function New-RunConfig {
    param([string]$RunID, [string]$Variant, [string]$Scenario)
    Invoke-Day6Kubectl -Arguments @('delete', 'configmap/edgeroute-k6-run', '-n', $namespace, '--ignore-not-found=true') | Out-Null
    $args = @('create', 'configmap', 'edgeroute-k6-run', '-n', $namespace,
        "--from-literal=RUN_ID=$RunID", "--from-literal=VARIANT=$Variant", "--from-literal=FAULT_SCENARIO=$Scenario",
        '--from-literal=HLS_BASE_URL=http://video.edgeroute.test:8080', '--from-literal=STALL_THRESHOLD_MS=1000')
    if ($Profile -eq 'smoke') {
        $args += @('--from-literal=TARGET_VUS=2', '--from-literal=PEAK_VUS=5', '--from-literal=PACE_SECONDS=0.2',
            '--from-literal=DNS_TTL=1s',
            '--from-literal=WARMUP_DURATION=2s', '--from-literal=STEADY_DURATION=6s', '--from-literal=RAMP_DURATION=2s',
            '--from-literal=PEAK_DURATION=6s', '--from-literal=RECOVERY_DURATION=2s')
    } else {
        $args += @('--from-literal=TARGET_VUS=20', '--from-literal=PEAK_VUS=100', '--from-literal=PACE_SECONDS=1',
            '--from-literal=DNS_TTL=30s',
            '--from-literal=WARMUP_DURATION=1m', '--from-literal=STEADY_DURATION=3m', '--from-literal=RAMP_DURATION=2m',
            '--from-literal=PEAK_DURATION=3m', '--from-literal=RECOVERY_DURATION=2m')
    }
    Invoke-Day6Kubectl -Arguments $args | Out-Null
}

function Save-ServiceMetrics {
    param([string]$RunDirectory)
    $core = (Invoke-Day6Kubectl -Arguments @('get', '--raw', '/api/v1/namespaces/edge-system/services/http:edgeroute-coredns:9153/proxy/metrics')) -join "`n"
    Save-Text -Path (Join-Path $RunDirectory 'coredns-metrics.txt') -Value $core
    $controllerOutput = & kubectl --context $Context get --raw '/api/v1/namespaces/edge-system/services/http:quality-controller:8080/proxy/metrics' 2>&1
    if ($LASTEXITCODE -eq 0) {
        $controller = $controllerOutput -join "`n"
    } else {
        $controller = "# unavailable for this variant`n# " + ($controllerOutput -join "`n")
    }
    Save-Text -Path (Join-Path $RunDirectory 'controller-metrics.txt') -Value $controller

    $query = [uri]::EscapeDataString('sum by (node,status,cache_status) (increase(nginxlog_http_response_count_total[5m]))')
    $prom = (Invoke-Day6Kubectl -Arguments @('get', '--raw', "/api/v1/namespaces/monitoring/services/http:monitoring-kube-prometheus-prometheus:9090/proxy/api/v1/query?query=$query")) -join "`n"
    Save-Text -Path (Join-Path $RunDirectory 'prometheus-range-query.json') -Value $prom
}

function Invoke-Run {
    param([string]$Variant, [string]$Scenario, [int]$Repetition, [string]$ImageTag)
    $runID = '{0}-{1}-{2:D2}-{3}' -f $Variant, $Scenario, $Repetition, (Get-Date).ToUniversalTime().ToString('yyyyMMddTHHmmssZ')
    $runDirectory = Join-Path $rawRoot $runID
    New-Item -ItemType Directory -Force $runDirectory | Out-Null
    $scenarioRoot = Join-Path $PSScriptRoot "scenarios\$Scenario"
    $startedAt = (Get-Date).ToUniversalTime()

    Reset-NodeQualityBaseline
    if ($Variant -eq 'adaptive') {
        Invoke-Day6Kubectl -Arguments @('scale', 'deployment/quality-controller', '-n', $namespace, '--replicas=1') | Out-Null
        Invoke-Day6Kubectl -Arguments @('rollout', 'status', 'deployment/quality-controller', '-n', $namespace, '--timeout=120s') | Out-Null
    }
    & (Join-Path $scenarioRoot 'setup.ps1')
    New-RunConfig -RunID $runID -Variant $Variant -Scenario $Scenario
    Invoke-Day6Kubectl -Arguments @('delete', 'job/edgeroute-k6', '-n', $namespace, '--ignore-not-found=true', '--wait=true') | Out-Null
    Install-K6Job
    $jobUID = ((Invoke-Day6Kubectl -Arguments @('get', 'job/edgeroute-k6', '-n', $namespace, '-o', 'jsonpath={.metadata.uid}')) -join '').Trim()
    if (-not $jobUID) { throw "Unable to resolve the k6 Job UID for $runID." }
    $podSelector = "batch.kubernetes.io/controller-uid=$jobUID"
    Invoke-Day6Kubectl -Arguments @('wait', '--for=condition=Ready', 'pod', '-n', $namespace, '-l', $podSelector, '--timeout=120s') | Out-Null
    $podNames = @((Invoke-Day6Kubectl -Arguments @('get', 'pods', '-n', $namespace, '-l', $podSelector, '-o', 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')) | Where-Object { $_.Trim() })
    if ($podNames.Count -ne 1) { throw "Expected exactly one pod for k6 Job UID $jobUID; found $($podNames.Count)." }
    $pod = $podNames[0].Trim()
    $podRunID = ((Invoke-Day6Kubectl -Arguments @('exec', '-n', $namespace, $pod, '--', 'printenv', 'RUN_ID')) -join '').Trim()
    if ($podRunID -ne $runID) { throw "k6 pod $pod has RUN_ID '$podRunID'; expected '$runID'." }
    Save-NodeQualitySnapshot -RunDirectory $runDirectory -Stage 'before-injection'
    Start-Sleep -Seconds $InjectionDelaySeconds
    & (Join-Path $scenarioRoot 'inject.ps1')
    & (Join-Path $scenarioRoot 'verify.ps1') -Expected Injected
    Save-NodeQualitySnapshot -RunDirectory $runDirectory -Stage 'after-injection'

    try {
        $deadline = (Get-Date).AddSeconds(900)
        do {
            & kubectl --context $Context exec -n $namespace $pod -- test -f /results/complete *> $null
            if ($LASTEXITCODE -eq 0) { break }
            if ((Get-Date) -ge $deadline) { throw "Timed out waiting for k6 result marker in pod $pod." }
            Start-Sleep -Seconds 2
        } while ($true)
        $relativeRunDirectory = [System.IO.Path]::GetRelativePath($repoRoot, $runDirectory).Replace('\', '/')
        Invoke-Day6Kubectl -Arguments @('cp', "$namespace/${pod}:/results/k6-summary.json", "$relativeRunDirectory/k6-summary.json") | Out-Null
        Invoke-Day6Kubectl -Arguments @('cp', "$namespace/${pod}:/results/k6-metrics.jsonl", "$relativeRunDirectory/k6-metrics.jsonl") | Out-Null
        $detailRunIDMatch = Get-Content -LiteralPath (Join-Path $runDirectory 'k6-metrics.jsonl') -TotalCount 100 |
            Select-String -Pattern '"run_id":"([^"]+)"' | Select-Object -First 1
        $detailRunID = if ($detailRunIDMatch) { $detailRunIDMatch.Matches[0].Groups[1].Value } else { '' }
        if ($detailRunID -ne $runID) { throw "Copied k6 detail has RUN_ID '$detailRunID'; expected '$runID'." }
        $k6ExitCode = ((Invoke-Day6Kubectl -Arguments @('exec', '-n', $namespace, $pod, '--', 'cat', '/results/exit-code')) -join '').Trim()
        $logs = (Invoke-Day6Kubectl -Arguments @('logs', "pod/$pod", '-n', $namespace)) -join "`n"
        Save-Text -Path (Join-Path $runDirectory 'k6.log') -Value $logs
        if ($k6ExitCode -ne '0') { throw "k6 returned exit code $k6ExitCode for $runID; raw output was retained." }
        Save-NodeQualitySnapshot -RunDirectory $runDirectory -Stage 'run-complete'
        Save-ServiceMetrics -RunDirectory $runDirectory
        try {
            $toxiproxyState = Get-ToxiproxyState -Node 'edge-syd-a'
        } catch {
            $toxiproxyState = (@{unavailable = $true; reason = $_.Exception.Message} | ConvertTo-Json -Compress)
        }
        Save-Text -Path (Join-Path $runDirectory 'toxiproxy-config.json') -Value $toxiproxyState
    } finally {
        & (Join-Path $scenarioRoot 'recover.ps1')
        & (Join-Path $scenarioRoot 'verify.ps1') -Expected Recovered
        Invoke-Day6Kubectl -Arguments @('delete', 'job/edgeroute-k6', '-n', $namespace, '--ignore-not-found=true', '--wait=true') | Out-Null
    }

    $finishedAt = (Get-Date).ToUniversalTime()
    $imageID = (& docker image inspect $ImageTag --format '{{.Id}}').Trim()
    $metadata = [ordered]@{
        run_id = $runID
        git_commit = $fullCommit
        variant = $Variant
        scenario = $Scenario
        repetition = $Repetition
        profile = $Profile
        random_seed = 20260828 + $Repetition
        job_uid = $jobUID
        k6_pod = $pod
        k6_exit_code = [int]$k6ExitCode
        start_timestamp = $startedAt.ToString('o')
        end_timestamp = $finishedAt.ToString('o')
        container_tags = @{coredns = $ImageTag; coredns_id = $imageID; k6_upstream = $K6Image; k6_runtime = $K6RuntimeImage}
        cluster_config = 'deploy/kind/cluster.yaml; 3 kind nodes; logical Sydney/Singapore regions'
        algorithm_config = switch ($Variant) {
            'baseline' { 'deterministic upstream modulo hash + active health + fallback' }
            'static-rendezvous' { 'equal-weight rendezvous + active health + fallback; NodeQuality controller disabled' }
            default { 'EWMA + ejection + weighted rendezvous + recovery ramp' }
        }
        injection_delay_seconds = $InjectionDelaySeconds
        host_hardware = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name).Trim()
    }
    Save-Text -Path (Join-Path $runDirectory 'experiment-metadata.json') -Value ($metadata | ConvertTo-Json -Depth 10)
    Write-Host "Completed $runID"
}

$callerLocation = Get-Location
Set-Location -LiteralPath $repoRoot
try {
    New-Item -ItemType Directory -Force $rawRoot | Out-Null
    Initialize-K6Resources
    foreach ($variant in $Variants) {
        $imageTag = Set-Variant -Variant $variant
        foreach ($scenario in $Scenarios) {
            for ($repetition = 1; $repetition -le $Repetitions; $repetition++) {
                Invoke-Run -Variant $variant -Scenario $scenario -Repetition $repetition -ImageTag $imageTag
            }
        }
    }
} finally {
    try { & (Join-Path $PSScriptRoot 'scenarios\latency\recover.ps1') } catch { Write-Warning $_ }
    try { & (Join-Path $PSScriptRoot 'scenarios\pod-down\recover.ps1') } catch { Write-Warning $_ }
    try { Set-Variant -Variant adaptive | Out-Null } catch { Write-Warning "Unable to restore adaptive mode: $_" }
    Set-Location -LiteralPath $callerLocation
}

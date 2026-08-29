[CmdletBinding()]
param(
    [string]$Namespace = 'edge-data',
    [string]$Context = 'kind-edgeroute',
    [string[]]$Edges = @('edge-syd-a', 'edge-syd-b', 'edge-sin-a')
)

$ErrorActionPreference = 'Stop'

function Invoke-KubectlExec {
    param(
        [string]$Deployment,
        [string[]]$Command
    )
    $output = & kubectl --context $Context exec -n $Namespace $Deployment -c nginx -- @Command 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl exec failed for ${Deployment}:`n$($output -join "`n")"
    }
    return $output
}

$metrics = Invoke-KubectlExec -Deployment deployment/edge-syd-a-edge `
    -Command @('wget', '-qO-', 'http://mediamtx:9998/metrics')
if (-not ($metrics -match 'paths\{name="live/demo",state="ready"\} 1')) {
    throw 'MediaMTX metrics do not report live/demo as ready.'
}

$results = foreach ($edge in $Edges) {
    $deployment = "deployment/$edge-edge"
    $expectedProxy = "$edge-origin"
    $proxyState = Invoke-KubectlExec -Deployment $deployment `
        -Command @('wget', '-qO-', 'http://127.0.0.1:8474/proxies')
    if (-not ($proxyState -match [regex]::Escape($expectedProxy))) {
        throw "Expected proxy $expectedProxy was not found."
    }

    $master = Invoke-KubectlExec -Deployment $deployment `
        -Command @('wget', '-qO-', 'http://127.0.0.1:8080/live/demo/index.m3u8?cookieCheck=1')
    $variant = @($master | Where-Object { $_ -match '^video.*\.m3u8' })[0]
    if (-not $variant) { throw "No video variant found through $edge." }

    $playlist = Invoke-KubectlExec -Deployment $deployment `
        -Command @('wget', '-qO-', "http://127.0.0.1:8080/live/demo/$variant")
    $segment = @($playlist | Where-Object { $_ -match '^[^#].*_seg[0-9]+\.mp4' } | Select-Object -Last 1)[0]
    if (-not $segment) { throw "No complete media segment found through $edge." }

    $url = "http://127.0.0.1:8080/live/demo/$segment"
    $first = Invoke-KubectlExec -Deployment $deployment -Command @('wget', '-S', '-O', '/dev/null', $url)
    $second = Invoke-KubectlExec -Deployment $deployment -Command @('wget', '-S', '-O', '/dev/null', $url)
    $firstStatus = @($first | Select-String 'X-Cache-Status:').Line.Trim()
    $secondStatus = @($second | Select-String 'X-Cache-Status:').Line.Trim()
    if ($firstStatus -ne 'X-Cache-Status: MISS' -or $secondStatus -ne 'X-Cache-Status: HIT') {
        throw "$edge cache transition was '$firstStatus' then '$secondStatus'."
    }

    [pscustomobject]@{
        Edge = $edge
        Proxy = $expectedProxy
        Playlist = 'PASS'
        First = 'MISS'
        Second = 'HIT'
    }
}

$results | Format-Table -AutoSize

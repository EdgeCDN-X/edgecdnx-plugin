[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

function Invoke-CheckedKubectl {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $output = & kubectl @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl $($Arguments -join ' ') failed:`n$($output -join "`n")"
    }
    return $output
}

function Invoke-ToxiproxyPost {
    param(
        [Parameter(Mandatory)][string]$Node,
        [Parameter(Mandatory)][string]$Path,
        [string]$Body = ''
    )
    $url = "http://127.0.0.1:8474$Path"
    $args = @('exec', '-n', 'edge-data', "deployment/$Node-edge", '-c', 'nginx', '--',
        'wget', '-qO-', '--header', 'Content-Type: application/json', '--post-data', $Body, $url)
    Invoke-CheckedKubectl -Arguments $args
}

function Get-ToxiproxyState {
    param([Parameter(Mandatory)][string]$Node)
    $args = @('exec', '-n', 'edge-data', "deployment/$Node-edge", '-c', 'nginx', '--',
        'wget', '-qO-', 'http://127.0.0.1:8474/proxies')
    return (Invoke-CheckedKubectl -Arguments $args) -join "`n"
}

function Reset-Toxiproxy {
    param([string]$Node = 'edge-syd-a')
    Invoke-ToxiproxyPost -Node $Node -Path '/reset' -Body '{}' | Out-Null
}

function Add-Toxic {
    param(
        [string]$Node = 'edge-syd-a',
        [Parameter(Mandatory)][string]$Json
    )
    Invoke-ToxiproxyPost -Node $Node -Path "/proxies/$Node-origin/toxics" -Body $Json | Out-Null
}

function Wait-EdgeReady {
    param([string]$Node = 'edge-syd-a', [int]$TimeoutSeconds = 120)
    Invoke-CheckedKubectl -Arguments @('rollout', 'status', "deployment/$Node-edge", '-n', 'edge-data', "--timeout=${TimeoutSeconds}s") | Out-Null
}

function Assert-Toxic {
    param([Parameter(Mandatory)][string]$Type, [string]$Node = 'edge-syd-a')
    $state = Get-ToxiproxyState -Node $Node
    if ($state -notmatch [regex]::Escape('"type":"' + $Type + '"')) {
        throw "Expected toxic type '$Type' was not found for $Node. State: $state"
    }
}

function Assert-NoToxics {
    param([string]$Node = 'edge-syd-a')
    $state = Get-ToxiproxyState -Node $Node
    if ($state -notmatch '"toxics":\[\]') {
        throw "Expected no toxics for $Node. State: $state"
    }
}

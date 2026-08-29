param([ValidateSet('Injected', 'Recovered')][string]$Expected = 'Injected')
. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
$ready = (Invoke-CheckedKubectl -Arguments @('get', 'deployment/edge-syd-a-edge', '-n', 'edge-data', '-o', 'jsonpath={.status.readyReplicas}')) -join ''
if ($Expected -eq 'Injected' -and $ready) { throw "Expected edge-syd-a to be down, but readyReplicas=$ready" }
if ($Expected -eq 'Recovered' -and $ready -ne '1') { throw "Expected one ready edge-syd-a replica, got '$ready'" }

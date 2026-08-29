param([ValidateSet('Injected', 'Recovered')][string]$Expected = 'Recovered')
. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Wait-EdgeReady -Node 'edge-syd-a'
$health = Invoke-CheckedKubectl -Arguments @('exec', '-n', 'edge-data', 'deployment/edge-syd-a-edge', '-c', 'nginx', '--', 'wget', '-qO-', 'http://127.0.0.1:8080/healthz')
if (($health -join '') -notmatch 'ok') { throw 'Recovered edge-syd-a did not pass its HTTP health check.' }

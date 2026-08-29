. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Invoke-CheckedKubectl -Arguments @('scale', 'deployment/edge-syd-a-edge', '-n', 'edge-data', '--replicas=1') | Out-Null
Wait-EdgeReady -Node 'edge-syd-a'
Reset-Toxiproxy -Node 'edge-syd-a'

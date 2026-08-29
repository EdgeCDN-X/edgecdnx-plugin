. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Invoke-CheckedKubectl -Arguments @('scale', 'deployment/edge-syd-a-edge', '-n', 'edge-data', '--replicas=0') | Out-Null
Invoke-CheckedKubectl -Arguments @('wait', '--for=delete', 'pod', '-n', 'edge-data', '-l', 'edgeroute.io/node=edge-syd-a', '--timeout=90s') | Out-Null

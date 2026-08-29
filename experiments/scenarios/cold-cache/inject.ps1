. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Invoke-CheckedKubectl -Arguments @('delete', 'pod', '-n', 'edge-data', '-l', 'edgeroute.io/node=edge-syd-a', '--wait=true') | Out-Null
Wait-EdgeReady -Node 'edge-syd-a'

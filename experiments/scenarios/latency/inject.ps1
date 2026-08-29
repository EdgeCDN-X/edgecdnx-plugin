. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
$toxic = '{"name":"day6-latency","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{"latency":150,"jitter":20}}'
Add-Toxic -Node 'edge-syd-a' -Json $toxic
Assert-Toxic -Node 'edge-syd-a' -Type 'latency'

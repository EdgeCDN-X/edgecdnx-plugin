. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
$toxic = '{"name":"day6-reset-peer","type":"reset_peer","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}'
Add-Toxic -Node 'edge-syd-a' -Json $toxic
Assert-Toxic -Node 'edge-syd-a' -Type 'reset_peer'

. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Wait-EdgeReady -Node 'edge-syd-a'
Reset-Toxiproxy -Node 'edge-syd-a'

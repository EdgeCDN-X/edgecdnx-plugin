. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
Reset-Toxiproxy -Node 'edge-syd-a'
Assert-NoToxics -Node 'edge-syd-a'

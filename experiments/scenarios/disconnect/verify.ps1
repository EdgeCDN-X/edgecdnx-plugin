param([ValidateSet('Injected', 'Recovered')][string]$Expected = 'Injected')
. (Join-Path $PSScriptRoot '..\..\lib\Day6.Common.ps1')
if ($Expected -eq 'Injected') { Assert-Toxic -Node 'edge-syd-a' -Type 'reset_peer' } else { Assert-NoToxics -Node 'edge-syd-a' }

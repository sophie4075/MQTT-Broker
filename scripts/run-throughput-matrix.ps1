<#
Drives the full flood-throughput matrix (QoS 0/1/2, client counts
1/10/50/100/200, optionally plus the 500-client QoS-2 special case, each
repeated 3 times) against ONE already-running broker, mirroring the
structure of the existing latency matrix (see scripts/run-load.ps1 and
scripts/analyze_results.py). Run it once per broker, switching which
broker process is active on the server in between.

Example:
    .\scripts\run-throughput-matrix.ps1 -Broker "tcp://192.168.2.111:1883" -BrokerName "BA-Broker" -BrokerKey "babroker"
#>
param(
    [Parameter(Mandatory = $true)][string]$Broker,
    [Parameter(Mandatory = $true)][string]$BrokerName,
    [Parameter(Mandatory = $true)][string]$BrokerKey,
    [double]$FloodDuration = 10,
    [string]$ResultsRoot = "results",
    [switch]$SkipSpecialCase
)

$qosLevels = 0, 1, 2
$clientCounts = 1, 10, 50, 100, 200
$repetitions = 1, 2, 3

function Run-Stage($qos, $clients, $rep) {
    $outDir = Join-Path $ResultsRoot "flood_$($BrokerKey)_qos$($qos)_n$($clients)_rep$($rep)"
    Write-Host "=== $BrokerName qos=$qos clients=$clients rep=$rep ==="
    & .\scripts\run-throughput.ps1 -Clients $clients -FloodDuration $FloodDuration `
        -Broker $Broker -Qos $qos -BrokerName $BrokerName -Repetition $rep `
        -OutDir $outDir
}

foreach ($rep in $repetitions) {
    foreach ($qos in $qosLevels) {
        foreach ($clients in $clientCounts) {
            Run-Stage $qos $clients $rep
        }
    }
    if (-not $SkipSpecialCase) {
        # 500-client special case, QoS 2 only
        Run-Stage 2 500 $rep
    }
}

Write-Host "=== $BrokerName flood matrix done ==="

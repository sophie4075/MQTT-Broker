<#
Starts several copies of latency-test.exe at once (one process per client)
and measures system throughput: total number of messages divided by the
time until all clients are done.

Each client gets its own ID and topic so they don't send messages to each
other. The broker must already be running separately (e.g. .\server.exe in
its own window).

Example:
    .\scripts\run-load.ps1 -Clients 10 -CountPerClient 100
#>
param(
    [int]$Clients = 10,
    [int]$CountPerClient = 100,
    [string]$Broker = "tcp://127.0.0.1:1883",
    [string]$Qos = 1,
    [string]$Timeout = "60s",
    [string]$OutDir = "results",
    [string]$Exe = ".\latency-test.exe",
    [string]$BrokerName = "",
    [string]$ThroughputLog = "results\throughput_summary.csv"
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$procs = @()
$start = Get-Date

for ($i = 1; $i -le $Clients; $i++) {
    $csv = Join-Path $OutDir "client$i.csv"
    $procs += Start-Process -FilePath $Exe -ArgumentList @(
        "-broker", $Broker,
        "-id", "client$i",
        "-topic", "bench/client$i",
        "-qos", $Qos,
        "-timeout", $Timeout,
        "-count", $CountPerClient,
        "-out", $csv
    ) -NoNewWindow -PassThru
}

$procs | Wait-Process

$elapsed = (Get-Date) - $start
$totalMessages = $Clients * $CountPerClient
$throughput = $totalMessages / $elapsed.TotalSeconds

Write-Host "Clients: $Clients, messages per client: $CountPerClient, total: $totalMessages"
Write-Host ("Elapsed: {0:N2} s" -f $elapsed.TotalSeconds)
Write-Host ("System throughput: {0:N1} messages/s" -f $throughput)
Write-Host "Per-client latency CSVs are in $OutDir\clientN.csv"

# Persist the system throughput so it survives even when stdout is
# suppressed (e.g. piping to $null for a large batch of stages). Numbers
# are formatted with InvariantCulture so the decimal separator is always
# "." regardless of the system locale (German Windows uses "," by default,
# which would break pandas' default CSV parsing downstream).
$invariant = [System.Globalization.CultureInfo]::InvariantCulture
$row = [PSCustomObject]@{
    broker               = $BrokerName
    qos                  = $Qos
    clients              = $Clients
    count_per_client     = $CountPerClient
    total_messages       = $totalMessages
    elapsed_s            = $elapsed.TotalSeconds.ToString($invariant)
    throughput_msgs_per_s = $throughput.ToString($invariant)
    out_dir              = $OutDir
}
$row | Export-Csv -Path $ThroughputLog -Append -NoTypeInformation

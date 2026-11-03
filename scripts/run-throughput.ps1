<#
Starts several copies of throughput-test.exe at once (one process per
client) and measures true system throughput: each client floods its own
topic as fast as possible (no waiting for a reply), and we sum up how many
messages arrived system-wide and over what wall-clock window. Unlike
run-load.ps1 (which uses the ping-pong latency-test.exe and is therefore
capped by round-trip time), this reflects the broker's actual
message-forwarding rate - see cmd/throughput-test/main.go.

Launching hundreds of processes takes Windows several seconds (observed:
~7s for 200 clients, worse for more). Each client floods for a fixed
DURATION (not a fixed message count - see cmd/throughput-test/main.go for
why). Without synchronizing the start, most clients would still be
spawning while early ones are already done flooding, so every client is
given a shared future -start-at timestamp: each one connects and
subscribes immediately, but waits until that instant before starting to
publish, so they all flood concurrently regardless of when their own
process happened to start.

No CSV is written by this script: scripts/analyze_results.py recomputes
throughput_flood_summary.csv directly from the flood_client*.csv files
this script produces, the same way it already does for the latency data -
an earlier version appended one summary row per stage via Export-Csv,
which silently breaks the moment a row's columns don't exactly match
whatever's already in the file.

The broker must already be running separately.

Example:
    .\scripts\run-throughput.ps1 -Clients 10 -FloodDuration 10 -Qos 0
#>
param(
    [int]$Clients = 10,
    [double]$FloodDuration = 10,
    [string]$Broker = "tcp://127.0.0.1:1883",
    [string]$Qos = 0,
    [string]$OutDir = "results",
    [string]$Exe = ".\throughput-test.exe",
    [string]$BrokerName = "",
    [int]$Repetition = 1
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Generous lead time so even the slowest-to-spawn client has connected and
# subscribed before the shared start instant arrives; scales with client
# count since spawning (and therefore connecting) more clients takes longer.
$leadSeconds = 5 + ($Clients * 0.1)
$startAt = (Get-Date).ToUniversalTime().AddSeconds($leadSeconds).ToString("yyyy-MM-ddTHH:mm:ss.fffffffK")

function Start-FloodClient($i, [string]$StartAtArg) {
    $csv = Join-Path $OutDir "flood_client$i.csv"
    Start-Process -FilePath $Exe -ArgumentList @(
        "-broker", $Broker,
        "-id", "flood-client$i",
        "-topic", "flood/client$i",
        "-qos", $Qos,
        "-duration", "${FloodDuration}s",
        "-start-at", $StartAtArg,
        "-out", $csv
    ) -NoNewWindow -PassThru
}

$start = Get-Date

$procs = @()
for ($i = 1; $i -le $Clients; $i++) {
    $procs += Start-FloodClient $i $startAt
    # A small stagger avoids overwhelming process creation itself (which
    # occasionally drops a handful of clients under Windows at 200+) - any
    # that still go missing are retried below anyway. The -start-at barrier
    # above is what makes the clients flood concurrently, not this stagger.
    if ($i % 20 -eq 0) { Start-Sleep -Milliseconds 50 }
}

$procs | Wait-Process

# Verify every client actually produced its summary file; retry any that
# didn't (process creation can intermittently fail under this burst load),
# up to a few attempts, before giving up on the rest.
for ($attempt = 1; $attempt -le 3; $attempt++) {
    $missing = @()
    for ($i = 1; $i -le $Clients; $i++) {
        if (-not (Test-Path (Join-Path $OutDir "flood_client$i.csv"))) { $missing += $i }
    }
    if ($missing.Count -eq 0) { break }
    Write-Host "Retry attempt $attempt for $($missing.Count) missing client(s): $($missing -join ',')"
    $retryStartAt = (Get-Date).ToUniversalTime().AddSeconds(3).ToString("yyyy-MM-ddTHH:mm:ss.fffffffK")
    $retryProcs = $missing | ForEach-Object { Start-FloodClient $_ $retryStartAt }
    $retryProcs | Wait-Process
}

$stillMissing = @()
for ($i = 1; $i -le $Clients; $i++) {
    if (-not (Test-Path (Join-Path $OutDir "flood_client$i.csv"))) { $stillMissing += $i }
}
if ($stillMissing.Count -gt 0) {
    Write-Warning "Giving up on $($stillMissing.Count) client(s) after retries: $($stillMissing -join ',')"
}

$elapsed = (Get-Date) - $start
Write-Host ("All $Clients clients done after {0:N2} s wall-clock" -f $elapsed.TotalSeconds)

# Quick console summary only (not written to disk) - real aggregation
# happens in analyze_results.py from the raw per-client CSVs.
$rows = for ($i = 1; $i -le $Clients; $i++) {
    $csv = Join-Path $OutDir "flood_client$i.csv"
    if (Test-Path $csv) { Import-Csv $csv }
}
$totalSent = ($rows | Measure-Object -Property sent -Sum).Sum
$totalUniqueReceived = ($rows | Measure-Object -Property unique_received -Sum).Sum
$totalMissing = ($rows | Measure-Object -Property missing -Sum).Sum
$totalPublishFailures = ($rows | Measure-Object -Property publish_failures -Sum).Sum
Write-Host "Clients: $Clients, flood duration: ${FloodDuration}s, total sent: $totalSent, total unique received: $totalUniqueReceived, missing: $totalMissing, publish failures: $totalPublishFailures"

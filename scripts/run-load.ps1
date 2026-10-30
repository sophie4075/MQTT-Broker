<#
Startet mehrere Kopien von latency-test.exe gleichzeitig (ein Prozess pro
Client) und misst den System-Durchsatz: Gesamtzahl Nachrichten geteilt
durch die Zeit, bis alle Clients fertig sind.

Jeder Client bekommt eine eigene ID und ein eigenes Topic, damit sie sich
nicht gegenseitig ihre Nachrichten zuschicken. Der Broker muss vorher
separat gestartet sein (z. B. .\server.exe in einem eigenen Fenster).

Beispiel:
    .\scripts\run-load.ps1 -Clients 10 -CountPerClient 100
#>
param(
    [int]$Clients = 10,
    [int]$CountPerClient = 100,
    [string]$Broker = "tcp://127.0.0.1:1883",
    [string]$Qos = 1,
    [string]$OutDir = "results",
    [string]$Exe = ".\latency-test.exe"
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
        "-count", $CountPerClient,
        "-out", $csv
    ) -NoNewWindow -PassThru
}

# wartet, bis wirklich jeder der $Clients Prozesse fertig ist
$procs | Wait-Process

$elapsed = (Get-Date) - $start
$totalMessages = $Clients * $CountPerClient

Write-Host "Clients: $Clients, Nachrichten pro Client: $CountPerClient, insgesamt: $totalMessages"
Write-Host ("Gesamtzeit: {0:N2} s" -f $elapsed.TotalSeconds)
Write-Host ("System-Durchsatz: {0:N1} Nachrichten/s" -f ($totalMessages / $elapsed.TotalSeconds))
Write-Host "Einzelne Latenz-CSVs liegen in $OutDir\clientN.csv"

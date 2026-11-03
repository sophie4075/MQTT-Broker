# BA-Broker

A minimal MQTT 3.1.1 broker written in Go.

## Requirements

- Go 1.26 or newer (https://go.dev/dl/)

## Run

Works the same on macOS, Linux and Windows:

```sh
go run ./cmd/server
```

The broker listens on port `1883` by default.

## Build

macOS / Linux:

```sh
go build -o ba-broker ./cmd/server
./ba-broker
```

Windows (PowerShell):

```powershell
go build -o ba-broker.exe ./cmd/server
.\ba-broker.exe
```

Cross-compile for a Raspberry Pi (64-bit OS):

```sh
GOOS=linux GOARCH=arm64 go build -o ba-broker ./cmd/server
```

On Windows (PowerShell):

```powershell
$env:GOOS="linux"; $env:GOARCH="arm64"; go build -o ba-broker ./cmd/server
```

## Options

| Flag           | Default | Description                                       |
|----------------|---------|---------------------------------------------------|
| `-addr`        | `:1883` | TCP listen address                                |
| `-latency-log` | `false` | Log server-internal publish/forward latency       |
| `-mem-log`     | `0`     | Interval for memory stats logging, e.g. `10s`     |
| `-pprof-addr`  | (off)   | Enable pprof endpoints, e.g. `:6060`              |

Example:

```sh
go run ./cmd/server -addr :1884
```

## Test

```sh
go test ./...
```

Quick manual check with the Mosquitto clients:

```sh
mosquitto_sub -h localhost -t test/topic
mosquitto_pub -h localhost -t test/topic -m "hello"
```

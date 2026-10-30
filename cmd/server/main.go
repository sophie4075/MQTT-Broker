package main

import (
	"BA-Broker/internal/server"
	"flag"
	"log"
	"log/slog"
	"os"
	"runtime"
	"time"
)

func main() {
	addr := flag.String("addr", ":1883", "TCP listen address")
	latencyLog := flag.Bool("latency-log", false,
		"log server-internal publish/forward latency")
	memLogInterval := flag.Duration("mem-log", 0,
		"interval for periodic runtime memory stats logging")
	flag.Parse()

	level := slog.LevelInfo
	if *latencyLog {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if *memLogInterval > 0 {
		go logMemStats(*memLogInterval)
	}

	srv := &server.Listener{
		Addr:   *addr,
		Server: server.New(),
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

// logMemStats periodically logs Go runtime memory/GC statistics, for comparison against
// sampled OS-level process metrics during resource-usage evaluation.
func logMemStats(interval time.Duration) {
	var m runtime.MemStats
	for {
		runtime.ReadMemStats(&m)
		slog.Info("memstats", "alloc", m.Alloc, "sys", m.Sys,
			"heap_objects", m.HeapObjects, "goroutines", runtime.NumGoroutine(), "numgc", m.NumGC)
		time.Sleep(interval)
	}
}

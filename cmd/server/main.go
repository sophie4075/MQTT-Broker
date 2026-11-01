package main

import (
	"BA-Broker/internal/server"
	"flag"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof/* handlers on http.DefaultServeMux
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
	pprofAddr := flag.String("pprof-addr", "",
		"address for the standard library's net/http/pprof debug endpoints, e.g. :6060 (empty disables it)")
	flag.Parse()

	level := slog.LevelInfo
	if *latencyLog {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if *memLogInterval > 0 {
		go logMemStats(*memLogInterval)
	}

	if *pprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				slog.Error("pprof server stopped", "err", err)
			}
		}()
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

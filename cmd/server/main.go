package main

import (
	"BA-Broker/internal/server"
	"log"
)

func main() {
	srv := &server.Listener{
		Addr:   ":1883",
		Server: server.New(),
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}

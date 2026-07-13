package main

import (
	"BA-Broker/internal/broker"
	"log"
)

func main() {
	srv := &broker.Server{
		Addr:   ":1883",
		Broker: broker.New(),
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("broker stopped: %v", err)
	}
}

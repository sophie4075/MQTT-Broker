// The idea is that a single client connects to the broker,
// subscribes to a topic, and then publishes messages to that same topic one
// after another. Because the client has subscribed to its own topic, the
// broker sends its own messages back to it. The time between "message sent"
// and "message received back" is the latency.
//
// For multiple concurrent clients, this same program is simply started several
// times at once (one process per client), each with its own client ID and topic.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "Adresse des Brokers")
	topic := flag.String("topic", "bench/latency", "Topic, auf dem getestet wird")
	clientID := flag.String("id", "latency-test", "Client-ID (muss pro gleichzeitigem Client eindeutig sein)")
	qos := flag.Int("qos", 1, "QoS-Stufe (0 oder 1)")
	count := flag.Int("count", 100, "Anzahl der Nachrichten")
	out := flag.String("out", "", "optionale CSV-Datei für die Latenz jeder einzelnen Nachricht")
	flag.Parse()

	// received is a kind of mailbox between two parts of the program,
	// soon as a message arrives from the server, the paho library calls the
	// function below in the background (SetDefaultPublishHandler). That
	// function writes the receive time into this channel. The main loop
	// below reads it back out. This is how the two parts of the program
	// "talk" to each other.
	received := make(chan time.Time, 1)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(*broker)
	opts.SetClientID(*clientID)
	opts.SetCleanSession(true)
	opts.SetDefaultPublishHandler(func(_ mqtt.Client, _ mqtt.Message) {
		received <- time.Now()
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Verbindung zum Broker fehlgeschlagen: %v", token.Error())
	}
	defer client.Disconnect(250)

	if token := client.Subscribe(*topic, byte(*qos), nil); token.Wait() && token.Error() != nil {
		log.Fatalf("Abonnieren fehlgeschlagen: %v", token.Error())
	}

	var csvFile *os.File
	if *out != "" {
		var err error
		csvFile, err = os.Create(*out)
		if err != nil {
			log.Fatalf("CSV-Datei konnte nicht angelegt werden: %v", err)
		}
		defer csvFile.Close()
		fmt.Fprintln(csvFile, "seq,latency_ms")
	}

	var total, minDuration, maxDuration time.Duration
	start := time.Now()

	for seq := 0; seq < *count; seq++ {
		sendTime := time.Now()

		payload := fmt.Sprintf("%d", seq)
		client.Publish(*topic, byte(*qos), false, payload)

		recvTime := <-received // blocks until our own message comes back
		latency := recvTime.Sub(sendTime)

		if seq == 0 || latency < minDuration {
			minDuration = latency
		}
		if latency > maxDuration {
			maxDuration = latency
		}
		total += latency

		if csvFile != nil {
			fmt.Fprintf(csvFile, "%d,%.3f\n", seq, latency.Seconds()*1000)
		}
	}

	elapsed := time.Since(start)
	avg := total / time.Duration(*count)

	fmt.Printf("client=%s topic=%s qos=%d count=%d\n", *clientID, *topic, *qos, *count)
	fmt.Printf("latenz minDuration/avg/maxDuration: %v / %v / %v\n", minDuration, avg, maxDuration)
	fmt.Printf("durchsatz: %.1f nachrichten/s\n", float64(*count)/elapsed.Seconds())
}

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type receivedMessage struct {
	seq int
	at  time.Time
}

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "broker address")
	topic := flag.String("topic", "bench/latency", "topic to test against")
	clientID := flag.String("id", "latency-test", "client ID")
	qos := flag.Int("qos", 1, "QoS level (0, 1, or 2)")
	count := flag.Int("count", 100, "number of messages")
	timeout := flag.Duration("timeout", 5*time.Second, "timeout per message")
	out := flag.String("out", "", "optional CSV file for the latency of each message")
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatal("qos must be between 0 and 2")
	}
	if *count <= 0 {
		log.Fatal("count must be greater than 0")
	}
	if *timeout <= 0 {
		log.Fatal("timeout must be greater than 0")
	}

	// Channel for messages received from the broker.
	received := make(chan receivedMessage, 1)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(*broker)
	opts.SetClientID(*clientID)
	opts.SetCleanSession(true)

	opts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		seq, err := strconv.Atoi(string(msg.Payload()))
		if err != nil {
			return
		}

		received <- receivedMessage{
			seq: seq,
			at:  time.Now(),
		}
	})

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("connecting to broker failed: %v", token.Error())
	}
	defer client.Disconnect(250)

	if token := client.Subscribe(*topic, byte(*qos), nil); token.Wait() && token.Error() != nil {
		log.Fatalf("subscribe failed: %v", token.Error())
	}

	var csvFile *os.File

	if *out != "" {
		var err error
		csvFile, err = os.Create(*out)
		if err != nil {
			log.Fatalf("could not create CSV file: %v", err)
		}
		defer csvFile.Close()

		fmt.Fprintln(csvFile, "seq,latency_ms")
	}

	var total time.Duration
	var minDuration, maxDuration time.Duration
	var elapsed time.Duration

	start := time.Now()

	for seq := 0; seq < *count; seq++ {
		sendTime := time.Now()
		deadline := sendTime.Add(*timeout)

		payload := fmt.Sprintf("%d", seq)
		client.Publish(*topic, byte(*qos), false, payload)

		var recvTime time.Time

		// Wait for the corresponding message.
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				log.Fatalf("timed out waiting for message %d", seq)
			}

			timer := time.NewTimer(remaining)

			select {
			case msg := <-received:
				timer.Stop()

				// Ignore messages belonging to previous publications.
				if msg.seq != seq {
					continue
				}

				recvTime = msg.at

			case <-timer.C:
				log.Fatalf("timed out waiting for message %d", seq)
			}

			if !recvTime.IsZero() {
				break
			}
		}

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

	elapsed = time.Since(start)
	avg := total / time.Duration(*count)

	fmt.Printf("client=%s topic=%s qos=%d count=%d\n", *clientID, *topic, *qos, *count)
	fmt.Printf("latency min/avg/max: %v / %v / %v\n", minDuration, avg, maxDuration)
	fmt.Printf("sequential throughput: %.1f messages/s\n", float64(*count)/elapsed.Seconds())
}

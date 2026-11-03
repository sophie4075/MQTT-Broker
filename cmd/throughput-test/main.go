// throughput-test measures the broker's actual message-forwarding rate,
// separate from round-trip latency: unlike latency-test's ping-pong design
// (publish, wait for the echo, publish next), this tool publishes as many
// messages as fast as possible without waiting for any reply, while
// counting arrivals on the receiving side. This mirrors the throughput
// methodology used in the muMQ paper (publishers flood the broker,
// subscribers count what arrives).
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "broker address")
	topic := flag.String("topic", "flood/client", "topic to test against")
	clientID := flag.String("id", "throughput-test", "client ID prefix (suffixed -pub/-sub)")
	qos := flag.Int("qos", 0, "QoS level (0, 1, or 2)")
	floodDuration := flag.Duration("duration", 10*time.Second,
		"how long to publish for, as fast as possible, before stopping and draining arrivals")
	drainTimeout := flag.Duration("drain-timeout", 15*time.Second,
		"how long to wait for in-flight messages to still arrive after the last new arrival")
	maxWait := flag.Duration("max-wait", 60*time.Second,
		"absolute cap on total wait time for arrivals, regardless of drain-timeout resets "+
			"(protects against a trickle of late messages keeping the process alive indefinitely)")
	startAt := flag.String("start-at", "", "optional RFC3339 timestamp; if set, connect and "+
		"subscribe immediately but wait until this instant before starting to publish, so that "+
		"many clients launched at staggered times (process spawning takes a while at high client "+
		"counts) still flood concurrently instead of one after another")
	out := flag.String("out", "", "optional one-row CSV summary output file")
	flag.Parse()

	if *qos < 0 || *qos > 2 {
		log.Fatal("qos must be between 0 and 2")
	}
	if *floodDuration <= 0 {
		log.Fatal("duration must be greater than 0")
	}

	// seen[seq] counts how many times sequence number seq arrived: missing
	// means lost, 1 means delivered exactly once, >1 means a duplicate
	// delivery (possible at QoS 1/2). The total message count isn't known
	// in advance with duration-based flooding, so this is a map (guarded
	// by mu) instead of a pre-sized slice; only the subscriber's handler
	// writes to it, so contention is limited to that one goroutine family.
	var mu sync.Mutex
	seen := make(map[uint64]uint32)
	var firstRecvNs, lastRecvNs int64 // unix nanos; 0 means "no arrival yet"

	subOpts := mqtt.NewClientOptions()
	subOpts.AddBroker(*broker)
	subOpts.SetClientID(*clientID + "-sub")
	subOpts.SetCleanSession(true)
	subOpts.SetProtocolVersion(4) // MQTT 3.1.1, matching the rest of the evaluation
	subOpts.SetAutoReconnect(false)
	subOpts.SetOrderMatters(false) // handler calls need not be serialized; we only record arrivals
	subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		now := time.Now().UnixNano()
		for {
			first := atomic.LoadInt64(&firstRecvNs)
			if first != 0 || !atomic.CompareAndSwapInt64(&firstRecvNs, 0, now) {
				break
			}
		}
		atomic.StoreInt64(&lastRecvNs, now)

		payload := msg.Payload()
		if len(payload) < 8 {
			return
		}
		seq := binary.BigEndian.Uint64(payload[:8])
		mu.Lock()
		seen[seq]++
		mu.Unlock()
	})

	subClient := mqtt.NewClient(subOpts)
	if token := subClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("subscriber connect failed: %v", token.Error())
	}
	defer subClient.Disconnect(250)

	if token := subClient.Subscribe(*topic, byte(*qos), nil); token.Wait() && token.Error() != nil {
		log.Fatalf("subscribe failed: %v", token.Error())
	}

	pubOpts := mqtt.NewClientOptions()
	pubOpts.AddBroker(*broker)
	pubOpts.SetClientID(*clientID + "-pub")
	pubOpts.SetCleanSession(true)
	pubOpts.SetProtocolVersion(4)
	pubOpts.SetAutoReconnect(false)

	pubClient := mqtt.NewClient(pubOpts)
	if token := pubClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("publisher connect failed: %v", token.Error())
	}
	defer pubClient.Disconnect(250)

	if *startAt != "" {
		t, err := time.Parse(time.RFC3339Nano, *startAt)
		if err != nil {
			log.Fatalf("invalid -start-at: %v", err)
		}
		if wait := time.Until(t); wait > 0 {
			time.Sleep(wait)
		} else {
			log.Printf("warning: -start-at was already %v in the past when ready to publish "+
				"(lead time too short for this client count); starting immediately", -wait)
		}
	}

	// Fire publishes as fast as possible (that is the point of a flood
	// test) without waiting in between, for a fixed duration, but keep
	// every token so completion can be verified afterwards - a Publish()
	// call returning does not mean the broker has accepted the message.
	tokens := make([]mqtt.Token, 0, 65536)
	sendStart := time.Now()
	sendDeadline := sendStart.Add(*floodDuration)
	var seq uint64
	for time.Now().Before(sendDeadline) {
		payload := make([]byte, 64)
		binary.BigEndian.PutUint64(payload[:8], seq)
		for i := 8; i < len(payload); i++ {
			payload[i] = 'x'
		}
		tokens = append(tokens, pubClient.Publish(*topic, byte(*qos), false, payload))
		seq++
	}
	totalSent := seq
	sendDone := time.Now()

	var publishFailures int
	for i, token := range tokens {
		if !token.WaitTimeout(30 * time.Second) {
			log.Printf("publish %d: timed out waiting for the broker to accept it", i)
			publishFailures++
			continue
		}
		if err := token.Error(); err != nil {
			log.Printf("publish %d failed: %v", i, err)
			publishFailures++
		}
	}
	publishVerifiedAt := time.Now()

	countReceived := func() int64 {
		mu.Lock()
		defer mu.Unlock()
		return int64(len(seen))
	}

	lastSeenCount := int64(-1)
	drainDeadline := time.Now().Add(*drainTimeout)
	hardDeadline := time.Now().Add(*maxWait)
	for {
		time.Sleep(50 * time.Millisecond)

		cur := countReceived()
		if cur >= int64(totalSent) {
			break
		}
		now := time.Now()
		if now.After(hardDeadline) {
			log.Printf("absolute max-wait (%v) exceeded: received %d/%d unique messages, giving up", *maxWait, cur, totalSent)
			break
		}
		if cur != lastSeenCount {
			lastSeenCount = cur
			drainDeadline = now.Add(*drainTimeout)
		} else if now.After(drainDeadline) {
			log.Printf("drain timed out: received %d/%d unique messages, giving up waiting for the rest", cur, totalSent)
			break
		}
	}

	var duplicates int64
	mu.Lock()
	uniqueReceived := int64(len(seen))
	for _, c := range seen {
		if c > 1 {
			duplicates += int64(c) - 1
		}
	}
	mu.Unlock()
	missing := int64(totalSent) - uniqueReceived

	first := atomic.LoadInt64(&firstRecvNs)
	last := atomic.LoadInt64(&lastRecvNs)

	var receiveWindow, totalElapsed time.Duration
	if first != 0 && last != 0 {
		receiveWindow = time.Duration(last - first)
		totalElapsed = time.Duration(last - sendStart.UnixNano())
	}
	sendDuration := sendDone.Sub(sendStart)
	publishVerifyDuration := publishVerifiedAt.Sub(sendDone)

	// totalElapsed (send start to last arrival) is the meaningful throughput
	// window for a single client: receiveWindow (first to last arrival) can
	// understate elapsed time and overstate throughput if many messages were
	// already published before the first echo came back.
	var throughput float64
	if totalElapsed > 0 {
		throughput = float64(uniqueReceived) / totalElapsed.Seconds()
	}

	fmt.Printf("client=%s topic=%s qos=%d sent=%d unique_received=%d missing=%d duplicates=%d publish_failures=%d\n",
		*clientID, *topic, *qos, totalSent, uniqueReceived, missing, duplicates, publishFailures)
	fmt.Printf("send duration (publish calls only): %.3fs, publish verification: %.3fs\n",
		sendDuration.Seconds(), publishVerifyDuration.Seconds())
	fmt.Printf("receive window (first to last arrival): %.3fs\n", receiveWindow.Seconds())
	fmt.Printf("total elapsed (send start to last arrival): %.3fs, throughput: %.1f msgs/s\n",
		totalElapsed.Seconds(), throughput)

	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatalf("could not create summary file: %v", err)
		}
		defer f.Close()
		fmt.Fprintln(f, "client_id,qos,sent,unique_received,missing,duplicates,publish_failures,"+
			"send_start_unix_ns,send_done_unix_ns,first_recv_unix_ns,last_recv_unix_ns")
		fmt.Fprintf(f, "%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			*clientID, *qos, totalSent, uniqueReceived, missing, duplicates, publishFailures,
			sendStart.UnixNano(), sendDone.UnixNano(), first, last)
	}
}

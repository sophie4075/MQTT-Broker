// lamp-rate-test drives the real lamp at a ramp of controlled, precise
// publish rates (not an unthrottled flood) and counts how many state
// updates actually arrive back, per rate stage. Unlike the synthetic
// throughput-test tool, the lamp doesn't echo a sequence number we can
// match 1:1, so this only gives an aggregate delivery ratio per stage
// (sent vs. received within that stage's time window), not per-message
// loss/duplicate detection.
//
// The point is not to measure the lamp's own maximum processing rate
// (that's a property of its hardware, roughly the same regardless of
// broker) but to compare how EACH BROKER behaves once the lamp - a real,
// slow, resource-constrained subscriber - falls behind: does it queue and
// drop individual messages while staying connected, or does it hit a
// write deadline and drop the whole connection (losing everything until
// the lamp reconnects)? That failure mode, and the rate at which it
// kicks in, differs by broker even though the lamp's own ceiling doesn't.
//
// Publisher and subscriber are two separate connections (same rationale
// as cmd/throughput-test), so this process's own send loop never starves
// its own receive handling.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "broker address")
	cmdTopic := flag.String("cmd-topic", "cmnd/proto-lamp/Color", "topic to publish rate-controlled commands to")
	resultTopic := flag.String("result-topic", "stat/proto-lamp/RESULT", "topic to subscribe to for arrival counting")
	ratesStr := flag.String("rates", "1,2,5,10,20,50", "comma-separated list of rates (messages/second) to ramp through")
	stageDuration := flag.Duration("stage-duration", 10*time.Second, "how long to hold each rate before moving to the next")
	qos := flag.Int("qos", 0, "QoS level for the published commands")
	clientID := flag.String("id", "lamp-rate-test", "client ID prefix (suffixed -pub/-sub)")
	out := flag.String("out", "", "CSV output path for per-stage results")
	settleQuiet := flag.Duration("settle-quiet", 3*time.Second,
		"before starting the ramp, wait until no arrivals have been seen for this long - drains any "+
			"backlog left over from a previous run (e.g. a prior rate=50 stage overwhelming the lamp) "+
			"so it doesn't get miscounted into this run's first stage")
	settleMax := flag.Duration("settle-max", 60*time.Second,
		"absolute cap on how long to wait for the quiet period above, in case arrivals never fully stop")
	flag.Parse()

	var rates []int
	for _, s := range strings.Split(*ratesStr, ",") {
		r, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			log.Fatalf("invalid rate %q: %v", s, err)
		}
		rates = append(rates, r)
	}

	// currentStageIdx tells the subscriber's handler which stage is active
	// right now, so arrivals get credited to the stage that was running
	// when they showed up (approximate - a response delayed across a
	// stage boundary gets counted in whichever stage happened to be
	// active when it arrived, not when it was requested).
	var currentStageIdx int32 = -1
	received := make([]int64, len(rates))

	var lastArrivalNs int64

	subOpts := mqtt.NewClientOptions()
	subOpts.AddBroker(*broker)
	subOpts.SetClientID(*clientID + "-sub")
	subOpts.SetCleanSession(true)
	subOpts.SetProtocolVersion(4)
	subOpts.SetAutoReconnect(false)
	subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, _ mqtt.Message) {
		atomic.StoreInt64(&lastArrivalNs, time.Now().UnixNano())
		idx := atomic.LoadInt32(&currentStageIdx)
		if idx >= 0 && int(idx) < len(rates) {
			atomic.AddInt64(&received[idx], 1)
		}
	})

	subClient := mqtt.NewClient(subOpts)
	if token := subClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("subscriber connect failed: %v", token.Error())
	}
	defer subClient.Disconnect(250)
	if token := subClient.Subscribe(*resultTopic, byte(*qos), nil); token.Wait() && token.Error() != nil {
		log.Fatalf("subscribe failed: %v", token.Error())
	}
	// Seed the quiet-period clock to subscribe time, not zero: zero would
	// mean "nothing has arrived yet in this process, therefore already
	// quiet," which falsely treats a backlog burst that just hasn't shown
	// up yet (lamp still catching up) as settled.
	atomic.StoreInt64(&lastArrivalNs, time.Now().UnixNano())

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

	// Wait for arrivals to go quiet before starting to count: a previous run
	// (or this run's own earlier rate stages) can leave the lamp with a
	// backlog of still-unsent state updates that trickle in late. Without
	// this, those late arrivals land while currentStageIdx already points at
	// the next stage and get miscounted there (observed: >100% "delivery" on
	// an immediately-following low-rate stage after a saturating one).
	settleStart := time.Now()
	settleDeadline := settleStart.Add(*settleMax)
	for {
		last := atomic.LoadInt64(&lastArrivalNs)
		quietSince := time.Since(time.Unix(0, last))
		if quietSince >= *settleQuiet {
			break
		}
		if time.Now().After(settleDeadline) {
			log.Printf("settle-max (%v) exceeded while waiting for arrivals to quiet down; starting anyway", *settleMax)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if settled := time.Since(settleStart); settled > time.Second {
		fmt.Printf("settled after %.1fs of quiet-waiting\n", settled.Seconds())
	}

	colors := []string{"FF0000", "00FF00", "0000FF", "FFFF00", "00FFFF", "FF00FF", "FFFFFF"}

	type stageResult struct {
		rate                    int
		sent, received          int64
		startUnixNs, endUnixNs  int64
	}
	var results []stageResult

	for i, rate := range rates {
		atomic.StoreInt32(&currentStageIdx, int32(i))
		interval := time.Second / time.Duration(rate)
		stageStart := time.Now()
		deadline := stageStart.Add(*stageDuration)

		var sent int64
		ticker := time.NewTicker(interval)
		fmt.Printf("=== rate=%d msg/s (interval=%v) ===\n", rate, interval)
		for range ticker.C {
			if time.Now().After(deadline) {
				break
			}
			color := colors[sent%int64(len(colors))]
			pubClient.Publish(*cmdTopic, byte(*qos), false, color)
			sent++
		}
		ticker.Stop()

		// Grace period so responses still in flight when the deadline hit
		// get credited to this stage rather than spilling into the next.
		time.Sleep(500 * time.Millisecond)
		stageEnd := time.Now()
		recv := atomic.LoadInt64(&received[i])

		var pct float64
		if sent > 0 {
			pct = 100 * float64(recv) / float64(sent)
		}
		fmt.Printf("rate=%d sent=%d received=%d delivery=%.1f%%\n", rate, sent, recv, pct)
		results = append(results, stageResult{rate, sent, recv, stageStart.UnixNano(), stageEnd.UnixNano()})
	}

	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatalf("could not create output: %v", err)
		}
		defer f.Close()
		fmt.Fprintln(f, "rate,sent,received,delivery_pct,start_unix_ns,end_unix_ns")
		for _, r := range results {
			var pct float64
			if r.sent > 0 {
				pct = 100 * float64(r.received) / float64(r.sent)
			}
			fmt.Fprintf(f, "%d,%d,%d,%.2f,%d,%d\n", r.rate, r.sent, r.received, pct, r.startUnixNs, r.endUnixNs)
		}
		fmt.Printf("wrote %s\n", *out)
	}
}

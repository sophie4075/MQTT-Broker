package server

import (
	"BA-Broker/internal/mqtt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestResolveSubscriberOfflineQueue(t *testing.T) {
	b := New()
	c := Client{id: "a", cleanSession: false}
	b.AddClient(&c)

	c.addSub("treehouse/monitor/greeting")
	err := b.topics.Subscribe("treehouse/monitor/greeting", c.id, mqtt.AtLeastOnce)
	if err != nil {
		t.Fatal(err)
	}
	b.RemoveClient(&c)

	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pub := &mqtt.Publish{
				TopicName: "treehouse/monitor/greeting",
				Payload:   []byte("hello, world"),
				QoS:       mqtt.AtLeastOnce,
			}
			b.resolveSubscriber(pub)
		}()
	}
	wg.Wait()
}

func TestResolveSubscriberQueuesMinQoS(t *testing.T) {
	b := New()
	err := b.topics.Subscribe("t", "x", mqtt.ExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	b.sessions["x"] = &session{}

	b.resolveSubscriber(&mqtt.Publish{
		TopicName: "t",
		QoS:       mqtt.AtLeastOnce,
		Payload:   []byte("hi"),
	})

	pending := b.sessions["x"].pendingPub
	if len(pending) != 1 {
		t.Fatalf("got %d pending messages, want 1", len(pending))
	}

	if pending[0].qos != mqtt.AtLeastOnce {
		t.Errorf("queued qos = %d, want %d (min(publish QoS, subscription QoS))", pending[0].qos, mqtt.AtLeastOnce)
	}

}

func TestResolveSubscriberSkipsEffectiveQoS0(t *testing.T) {
	b := New()
	err := b.topics.Subscribe("t", "x", mqtt.ExactlyOnce)
	if err != nil {
		t.Fatal(err)
	}
	b.sessions["x"] = &session{}

	b.resolveSubscriber(&mqtt.Publish{
		TopicName: "t",
		QoS:       mqtt.AtMostOnce,
		Payload:   []byte("hi"),
	})

	if got := len(b.sessions["x"].pendingPub); got != 0 {
		t.Errorf("got %d pending messages, want 0 (effective QoS is 0, nothing to persist)", got)
	}
}

// when a client reconnects with the same ID, the OLD connection's cleanup
// goroutine is still running concurrently and will eventually call
// RemoveClient. Since the topic tree is keyed by ClientID that late cleanup must not be allowed
// to remove subscriptions that the NEW connection already re-created under the same ID.
func TestRemoveClientTakeoverPreservesNewSubscription(t *testing.T) {
	conn1, _ := net.Pipe()
	oldC := &Client{id: "x", cleanSession: true, conn: conn1}
	b := New()
	b.AddClient(oldC)
	oldC.addSub("foo/bar")
	err := b.topics.Subscribe("foo/bar", "x", mqtt.AtMostOnce)

	if err != nil {
		t.Fatal(err)
	}

	// A new Client with the same ID connects and re-subscribes
	newC := &Client{id: "x", cleanSession: true}
	b.AddClient(newC)
	newC.addSub("foo/bar")

	err = b.topics.Subscribe("foo/bar", "x", mqtt.AtMostOnce)
	if err != nil {
		t.Fatal(err)
	}

	b.RemoveClient(oldC)

	// The new connection's subscription must have survived the old
	// connection's (late) cleanup.
	got := b.topics.Match("foo/bar")
	if _, ok := got["x"]; !ok {
		t.Errorf("Match(%q) = %v, want subscriber %q still present after old connection's cleanup", "foo/bar", got, "x")
	}
}

func TestPendingQoS2PersistsAcrossReconnect(t *testing.T) {
	conn1, _ := net.Pipe()
	b := New()
	c := &Client{id: "x", cleanSession: false, conn: conn1}
	b.AddClient(c)
	c.pendingQoS(5)

	b.RemoveClient(c)

	//Simulate client reconnected
	c2 := &Client{id: "x", cleanSession: false}
	b.AddClient(c2)

	if c2.pendingQoS(5) {
		t.Errorf("pendingQoS(5) on reconnected client = true, want false: " +
			"the client already sent this QoS2 message once before disconnecting, " +
			"a resend with DUP=1 must not be forwarded to subscribers a second time")
	}
}

func TestReconnectResendOrder(t *testing.T) {
	for trial := 0; trial < 30; trial++ {
		conn, _ := net.Pipe()
		old := &Client{id: "x", cleanSession: false, conn: conn}

		topics := []string{"a", "b", "c", "d", "e"}
		for i, topicName := range topics {
			old.trackPub(uint16(i+1), pendingPublish{topic: topicName, qos: mqtt.AtLeastOnce})
		}

		b := New()
		b.clients["x"] = old

		newC := &Client{id: "x", cleanSession: false}
		_, pendingPub := b.AddClient(newC)

		if len(pendingPub) != len(topics) {
			t.Fatalf("trial %d: got %d pending messages, want %d", trial, len(pendingPub), len(topics))
		}
		for i, want := range topics {
			if pendingPub[i].topic != want {
				t.Fatalf("trial %d: pendingPub[%d].topic = %q, want %q (original send order not preserved)", trial, i, pendingPub[i].topic, want)
			}
		}
	}
}

func TestHandleSubscribeSendsSubackBeforeRetained(t *testing.T) {
	conn1, conn2 := net.Pipe()
	b := New()
	b.storeRetained(&mqtt.Publish{TopicName: "t", Payload: []byte("hi"), Retain: true})

	c := &Client{id: "x", conn: conn1}

	done := make(chan error, 1)
	go func() {
		done <- b.handleSubscribe(c, &mqtt.Subscribe{
			PacketID: 1,
			Topics:   []mqtt.TopicSubscription{{Topic: "t", QoS: mqtt.AtMostOnce}},
		})
	}()

	first := readPacketType(t, conn2)
	if first != mqtt.SUBACK {
		t.Errorf("first packet sent = type %d, want SUBACK (%d); retained messages must not be sent before the SUBACK", first, mqtt.SUBACK)
	}

	readPacketType(t, conn2) // drain the second packet so the goroutine can finish

	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func readPacketType(t *testing.T, r io.Reader) mqtt.PacketType {
	t.Helper()
	headerByte := make([]byte, 1)
	if _, err := io.ReadFull(r, headerByte); err != nil {
		t.Fatal(err)
	}
	pktType := mqtt.PacketType(headerByte[0] >> 4)

	remaining, err := mqtt.DecodeLength(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, r, int64(remaining)); err != nil {
		t.Fatal(err)
	}
	return pktType
}

func TestWriteHasDeadline(t *testing.T) {
	conn1, _ := net.Pipe()
	c := &Client{conn: conn1}

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.write(func(w io.Writer) error {
			_, err := w.Write([]byte("hello"))
			return err
		})
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("write succeeded with no reader on the other end, want a deadline error")
		}
	case <-time.After(writeTimeout + 100*time.Millisecond):
		t.Error("write blocked past 200ms, want it to time out via a write deadline")
	}
}

func TestReconnectResendsPubrelAfterPubrec(t *testing.T) {
	b := New()

	conn1, conn1Peer := net.Pipe()
	c := &Client{id: "x", cleanSession: false, conn: conn1}
	b.AddClient(c)

	id := c.allocPacketID()
	c.trackPub(id, pendingPublish{topic: "t", qos: mqtt.ExactlyOnce})

	ackDone := make(chan error, 1)
	go func() {
		ackDone <- b.handleAck(c, &mqtt.Ack{Kind: mqtt.PUBREC, PacketID: id})
	}()
	readPacketType(t, conn1Peer)
	if err := <-ackDone; err != nil {
		t.Fatal(err)
	}

	b.RemoveClient(c) // Connection closed, session gets stored

	// Reconnect
	conn2, conn2Peer := net.Pipe()
	c2 := &Client{id: "x", conn: conn2}

	connectDone := make(chan error, 1)
	go func() {
		connectDone <- b.HandlePacket(c2, &mqtt.Connect{
			ProtocolLevel: SupportedProtocolLevel,
			Payload:       mqtt.ConnectPayload{ClientID: "x"},
		})
	}()

	first := readPacketType(t, conn2Peer) // CONNACK
	if first != mqtt.CONNACK {
		t.Fatalf("first packet = type %d, want CONNACK (%d)", first, mqtt.CONNACK)
	}

	second := readPacketType(t, conn2Peer) // Resend of the tracked message
	if second != mqtt.PUBREL {
		t.Errorf("resend after reconnect = type %d, want PUBREL (%d): PUBREC was already received before the disconnect", second, mqtt.PUBREL)
	}

	if err := <-connectDone; err != nil {
		t.Fatal(err)
	}
}

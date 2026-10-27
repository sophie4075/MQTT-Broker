package broker

import (
	"BA-Broker/internal/mqtt"
	"sync"
	"testing"
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

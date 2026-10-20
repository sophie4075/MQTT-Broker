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

package broker

import (
	"BA-Broker/internal/topic"
	"io"
	"log"

	"BA-Broker/internal/mqtt"
)

// retainedMsg is the last retained Application Message stored for a topic
// name. See MQTT-3.3.1-5.
type retainedMsg struct {
	payload []byte
	qos     mqtt.QoS
}

type retainedPair struct {
	topicName string
	msg       retainedMsg
}

// subscriber is a subscription resolved to its connected Client, ready for
// delivery.
type subscriber struct {
	id   string
	dest *Client
	qos  mqtt.QoS
}

func (b *Broker) handlePublish(c *Client, p *mqtt.Publish) error {
	// see Table 3.4 - Expected Publish Packet response / MQTT-3.3.4-1
	switch p.QoS {
	case mqtt.AtLeastOnce:
		if err := c.write(func(w io.Writer) error {
			return mqtt.WriteAck(w, mqtt.PUBACK, *p.PacketID)
		}); err != nil {
			return err
		}
	case mqtt.ExactlyOnce:
		if err := c.write(func(w io.Writer) error {
			return mqtt.WriteAck(w, mqtt.PUBREC, *p.PacketID)
		}); err != nil {
			return err
		}
		// TODO: QoS 2 handshake
	}

	b.storeRetained(p)

	for _, t := range b.resolveSubscriber(p.TopicName) {
		out := buildOutgoingPublish(p, t.dest, t.qos, false)
		b.forward(t.id, t.dest, out)
	}
	return nil
}

// storeRetained applies a PUBLISH's RETAIN flag to the broker's retained-message store
// TODO MQTT-3.3.1-6
func (b *Broker) storeRetained(p *mqtt.Publish) {
	if !p.Retain {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if len(p.Payload) == 0 {
		// A zero-byte retained payload clears any
		// existing retained message and is never stored itself (see MQTT-3.3.1-10/-11).
		delete(b.retained, p.TopicName)
		return
	}
	// MQTT-3.3.1-5/-7: store as the new retained message, replacing
	// whatever (if anything) was retained for this topic before.
	b.retained[p.TopicName] = retainedMsg{payload: p.Payload, qos: p.QoS}
}

// resolveSubscriber matches subscribers and resolves them to clients
func (b *Broker) resolveSubscriber(topic string) []subscriber {
	subs := b.topics.Match(topic)

	b.mu.RLock()
	defer b.mu.RUnlock()

	targets := make([]subscriber, 0, len(subs))
	for id, qos := range subs {
		if dest, ok := b.clients[id]; ok {
			targets = append(targets, subscriber{id, dest, qos})
		}
	}
	return targets
}

func buildOutgoingPublish(p *mqtt.Publish, dest *Client, subQoS mqtt.QoS, retain bool) *mqtt.Publish {
	outQoS := min(p.QoS, subQoS)
	out := &mqtt.Publish{
		TopicName: p.TopicName,
		Payload:   p.Payload,
		QoS:       outQoS,
		Dup:       false, // TODO: DUP Tracking
		Retain:    retain,
	}
	if outQoS > 0 {
		id := dest.allocPacketID()
		out.PacketID = &id
	}
	return out
}

// forward writes a Publish to a single subscriber.
func (b *Broker) forward(id string, dest *Client, out *mqtt.Publish) {
	if err := dest.write(func(w io.Writer) error {
		return mqtt.WritePublish(w, out)
	}); err != nil {
		log.Printf("publish to %q failed: %v", id, err)
	}
}

func (b *Broker) sendRetained(c *Client, filter string, subQoS mqtt.QoS) {
	b.mu.RLock()
	var matches []retainedPair
	for topicName, msg := range b.retained {
		if topic.Matches(filter, topicName) {
			matches = append(matches, retainedPair{topicName, msg})
		}
	}
	b.mu.RUnlock()

	for _, m := range matches {
		publish := buildOutgoingPublish(
			&mqtt.Publish{
				TopicName: m.topicName,
				Payload:   m.msg.payload,
				QoS:       m.msg.qos,
			},
			c,
			subQoS,
			true)
		b.forward(c.id, c, publish)
	}

}

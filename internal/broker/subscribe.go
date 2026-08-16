package broker

import (
	"io"
	"log"

	"BA-Broker/internal/mqtt"
)

func (b *Broker) handleSubscribe(c *Client, p *mqtt.Subscribe) error {
	log.Printf("SUBSCRIBE Received from %q", c.id)
	// See [MQTT-3.9.3-2]
	rc := make([]byte, 0, len(p.Topics))
	for _, t := range p.Topics {
		err := b.topics.Subscribe(t.Topic, c.id, t.QoS)
		if err != nil {
			log.Printf("Error subscribing to topic %q: %v", t.Topic, err)
			// failure
			rc = append(rc, 0x80)
			continue
		}
		// 0x00, 0x01, or 0x02
		rc = append(rc, byte(t.QoS))
		b.sendRetained(c, t.Topic, t.QoS) // MQTT-3.3.1-6
	}
	// TODO make sure to double check 3.8.4 Response (handle Topics correctly)
	return c.write(func(w io.Writer) error {
		return mqtt.WriteSuback(w, p.PacketID, rc)
	})
}

func (b *Broker) handleUnsubscribe(c *Client, p *mqtt.Unsubscribe) error {
	for _, t := range p.Topics {
		b.topics.Unsubscribe(t, c.id)
	}
	return c.write(func(w io.Writer) error {
		return mqtt.WriteAck(w, p.Type(), p.PacketID)
	})
}

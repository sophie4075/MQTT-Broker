package broker

import (
	"BA-Broker/internal/mqtt"
	"io"
)

// handleAck dispatches PUBACK/PUBREC/PUBREL/PUBCOMP
func (b *Broker) handleAck(c *Client, p *mqtt.Ack) error {
	switch p.Kind {
	case mqtt.PUBREL:
		c.clearPendingQoS(p.PacketID)
		return c.write(func(w io.Writer) error {
			return mqtt.WriteAck(w, mqtt.PUBCOMP, p.PacketID)
		})
	default:
		// TODO: sender-side QoS handling once outgoing delivery tracks in-flight state.
		return nil
	}
}

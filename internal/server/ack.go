package server

import (
	"BA-Broker/internal/mqtt"
	"io"
)

// handleAck dispatches PUBACK/PUBREC/PUBREL/PUBCOMP
func (s *Server) handleAck(c *Client, p *mqtt.Ack) error {
	switch p.Kind {
	case mqtt.PUBREL:
		c.clearPendingQoS(p.PacketID)
		return c.write(func(w io.Writer) error {
			return mqtt.WriteAck(w, mqtt.PUBCOMP, p.PacketID)
		})
	case mqtt.PUBREC:
		c.markPubrecReceived(p.PacketID)
		return c.write(func(w io.Writer) error {
			return mqtt.WriteAck(w, mqtt.PUBREL, p.PacketID)
		})
	case mqtt.PUBACK, mqtt.PUBCOMP:
		c.clearTracking(p.PacketID)
		return nil
	default:
		return nil
	}
}

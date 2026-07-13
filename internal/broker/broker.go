package broker

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"BA-Broker/internal/mqtt"
)

// errClientDisconnect signals an orderly DISCONNECT.
var errClientDisconnect = errors.New("client disconnected")

// Broker owns MQTT state (e.g: clients, topics, subscriptions).
type Broker struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func New() *Broker {
	return &Broker{
		clients: make(map[string]*Client),
	}
}

// Client represents a connected MQTT client.
type Client struct {
	conn    net.Conn
	id      string
	writeMu sync.Mutex
}

// write ensures only one goroutine writes to the connection at a time.
func (c *Client) write(fn func(io.Writer) error) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return fn(c.conn)
}

// AddClient registers a new client.
func (b *Broker) AddClient(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO: if an existing client has this ID, disconnect the old one (MQTT-3.1.4-2).
	b.clients[c.id] = c
}

// RemoveClient removes a client if it is still registered under a specific ID.
func (b *Broker) RemoveClient(c *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.clients[c.id] == c {
		delete(b.clients, c.id)
	}
}

// HandlePacket dispatches a decoded packet.
func (b *Broker) HandlePacket(c *Client, pkt mqtt.Packet) error {
	switch p := pkt.(type) {
	case *mqtt.Connect:
		// TODO: enforce that CONNECT is the first packet, reject duplicates,
		// validate/assign the client ID, and track the session.
		log.Printf("mqtt: CONNECT id=%q clean=%v keepalive=%d",
			p.Payload.ClientID, p.Flags.CleanSession, p.KeepAlive)
		return c.write(func(w io.Writer) error {
			return mqtt.WriteConnack(w, false, 0x00)
		})
	case *mqtt.Publish:
		return b.handlePublish(c, p)

	case *mqtt.Subscribe:
		return b.handleSubscribe(c, p)

	case *mqtt.Unsubscribe:
		return b.handleUnsubscribe(c, p)

	case *mqtt.Pingreq:
		return c.write(mqtt.WritePingresp)

	case *mqtt.Disconnect:
		log.Printf("client disconnected: %q", c.id)
		return errClientDisconnect

	default:
		return fmt.Errorf("unexpected packet type %d", pkt.Type())
	}
}

func (b *Broker) handlePublish(c *Client, p *mqtt.Publish) error {
	// TODO: topic routing + QoS handling
	log.Printf("PUBLISH from %q topic=%q", c.id, p.TopicName)
	return nil
}

func (b *Broker) handleSubscribe(c *Client, p *mqtt.Subscribe) error {
	// TODO: subscription registry + SUBACK
	log.Printf("SUBSCRIBE from %q", c.id)
	return nil
}

func (b *Broker) handleUnsubscribe(c *Client, p *mqtt.Unsubscribe) error {
	// TODO: unsubscribe logic + UNSUBACK
	log.Printf("UNSUBSCRIBE from %q", c.id)
	return nil
}

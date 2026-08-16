package broker

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"BA-Broker/internal/mqtt"
	"BA-Broker/internal/topic"
)

// errClientDisconnect signals an orderly DISCONNECT.
var errClientDisconnect = errors.New("client disconnected")
var errIdentifierRejected = errors.New("identifier rejected")

// Broker owns MQTT state (e.g: clients, topics, subscriptions).
type Broker struct {
	mu       sync.RWMutex
	clients  map[string]*Client
	topics   *topic.Tree
	retained map[string]retainedMsg // topic name -> last retained message
}

func New() *Broker {
	return &Broker{
		clients: make(map[string]*Client),
		topics:  topic.NewTree(),
		// TODO: eventually retainedMu sync.RWMutex? To avoid one lock covering two unrelated maps
		retained: make(map[string]retainedMsg),
	}
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
		c.id = p.Payload.ClientID
		// see MQTT 3.1.3.1
		if c.id == "" {
			if err := c.write(func(w io.Writer) error {
				return mqtt.WriteConnack(w, false, 0x02)
			}); err != nil {
				return err
			}
			log.Println("Connection Refused, identifier rejected as it is empty")
			return errIdentifierRejected
		}

		b.AddClient(c)
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

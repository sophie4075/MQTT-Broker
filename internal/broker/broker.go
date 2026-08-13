package broker

import (
	"BA-Broker/internal/topic"
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
var errIdentifierRejected = errors.New("identifier rejected")

// Broker owns MQTT state (e.g: clients, topics, subscriptions).
type Broker struct {
	mu      sync.RWMutex
	clients map[string]*Client
	topics  *topic.Tree
}

func New() *Broker {
	return &Broker{
		clients: make(map[string]*Client),
		topics:  topic.NewTree(),
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

func (b *Broker) handlePublish(c *Client, p *mqtt.Publish) error {
	// TODO: topic routing + QoS handling
	log.Printf("PUBLISH from %q topic=%q", c.id, p.TopicName)
	return nil
}

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

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

type session struct {
	subs map[string]struct{}
}

// Broker owns MQTT state (e.g: clients, topics, subscriptions).
type Broker struct {
	mu       sync.RWMutex
	clients  map[string]*Client
	sessions map[string]*session
	topics   *topic.Tree
	retained map[string]retainedMsg // topic name -> last retained message
}

func New() *Broker {
	return &Broker{
		clients:  make(map[string]*Client),
		sessions: make(map[string]*session),
		topics:   topic.NewTree(),
		// TODO: eventually retainedMu sync.RWMutex? To avoid one lock covering two unrelated maps
		retained: make(map[string]retainedMsg),
	}
}

// AddClient registers a new client.
func (b *Broker) AddClient(c *Client) (sessionPresent bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if old, exists := b.clients[c.id]; exists {
		// TODO add error handling
		old.conn.Close()
	}

	sess, ok := b.sessions[c.id]

	if !c.cleanSession && ok {
		c.subs = sess.subs
		b.clients[c.id] = c
		return true
	}

	if c.cleanSession && ok {
		for filter := range sess.subs {
			b.topics.Unsubscribe(filter, c.id)
		}
		delete(b.sessions, c.id)
	}

	c.subs = make(map[string]struct{})
	b.clients[c.id] = c
	return false
}

// RemoveClient removes a client if it is still registered under a specific ID.
func (b *Broker) RemoveClient(c *Client) {
	if c.cleanSession {
		for filter := range c.subs {
			b.topics.Unsubscribe(filter, c.id)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.clients[c.id] != c {
		return
	}

	if !c.cleanSession {
		b.sessions[c.id] = &session{subs: c.subs}
	}
	delete(b.clients, c.id)
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
		c.cleanSession = p.Flags.CleanSession

		sessionPresent := b.AddClient(c)
		log.Printf("mqtt: CONNECT id=%q clean=%v keepalive=%d",
			p.Payload.ClientID, p.Flags.CleanSession, p.KeepAlive)
		return c.write(func(w io.Writer) error {
			return mqtt.WriteConnack(w, sessionPresent, 0x00)
		})
	case *mqtt.Publish:
		return b.handlePublish(c, p)

	case *mqtt.Subscribe:
		return b.handleSubscribe(c, p)

	case *mqtt.Unsubscribe:
		return b.handleUnsubscribe(c, p)
	case *mqtt.Ack:
		return b.handleAck(c, p)
	case *mqtt.Pingreq:
		return c.write(mqtt.WritePingresp)

	case *mqtt.Disconnect:
		log.Printf("client disconnected: %q", c.id)
		return errClientDisconnect

	default:
		return fmt.Errorf("unexpected packet type %d", pkt.Type())
	}
}

package broker

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"BA-Broker/internal/mqtt"
	"BA-Broker/internal/topic"
)

// errClientDisconnect signals an orderly DISCONNECT.
var errClientDisconnect = errors.New("client disconnected")
var errIdentifierRejected = errors.New("identifier rejected")
var errUnsupportedProtocol = errors.New("unsupported protocol level")

const maxPendingPerSession = 50
const SupportedProtocolLevel = 4

type pendingPublish struct {
	topic    string
	payload  []byte
	qos      mqtt.QoS
	retain   bool
	packetID uint16
	dup      bool
}
type session struct {
	subs       map[string]struct{}
	pendingPub []pendingPublish
	nextPktID  uint16
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
func (b *Broker) AddClient(c *Client) (sessionPresent bool, pendingPub []pendingPublish) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if old, exists := b.clients[c.id]; exists {
		// TODO add error handling
		old.conn.Close()
		if !old.cleanSession {
			sess := &session{
				subs:      old.snapshotSubs(),
				nextPktID: old.snapshotPktID(),
			}
			for id, pub := range old.snapshotPub() {
				pub.dup = true
				pub.packetID = id
				sess.enqueue(pub)
			}
			b.sessions[c.id] = sess
		}
	}

	sess, ok := b.sessions[c.id]

	if !c.cleanSession && ok {
		c.subs = sess.subs
		c.nextPktID = sess.nextPktID
		pendingPub = sess.pendingPub
		sess.pendingPub = nil
		log.Printf("restored session for %q: subs=%v", c.id, c.subs)
		b.clients[c.id] = c
		return true, pendingPub
	}

	if c.cleanSession && ok {
		for filter := range sess.subs {
			b.topics.Unsubscribe(filter, c.id)
		}
		delete(b.sessions, c.id)
	}

	c.subs = make(map[string]struct{})
	b.clients[c.id] = c
	return false, nil
}

// RemoveClient removes a client if it is still registered under a specific ID.
func (b *Broker) RemoveClient(c *Client) {
	subs := c.snapshotSubs()
	pubs := c.snapshotPub()
	nextID := c.snapshotPktID()

	if c.cleanSession {
		for filter := range subs {
			b.topics.Unsubscribe(filter, c.id)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.clients[c.id] != c {
		return
	}

	if !c.cleanSession {
		sess := &session{
			subs:      subs,
			nextPktID: nextID,
		}
		for id, pub := range pubs {
			pub.dup = true
			pub.packetID = id
			sess.enqueue(pub)
		}
		b.sessions[c.id] = sess
	}
	delete(b.clients, c.id)
}

// HandlePacket dispatches a decoded packet.
func (b *Broker) HandlePacket(c *Client, pkt mqtt.Packet) error {
	switch p := pkt.(type) {
	case *mqtt.Connect:
		if p.ProtocolLevel != SupportedProtocolLevel {
			if err := c.write(func(w io.Writer) error {
				return mqtt.WriteConnack(w, false, 0x01)
			}); err != nil {
				return err
			}
			log.Printf("Connection Refused, unsupported protocol level %d", p.ProtocolLevel)
			return errUnsupportedProtocol
		}
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

		if p.KeepAlive > 0 {
			c.readTimeout = time.Duration(p.KeepAlive) * time.Second * 3 / 2
		}

		if p.Flags.WillFlag {
			c.will = &willMessage{
				topic:   p.Payload.WillTopic,
				payload: p.Payload.WillMsg,
				qos:     p.Flags.WillQoS,
				retain:  p.Flags.WillRetain,
			}
		}

		sessionPresent, pendingPub := b.AddClient(c)
		log.Printf("mqtt: CONNECT id=%q clean=%v session_present=%v keepalive=%d",
			p.Payload.ClientID, p.Flags.CleanSession, sessionPresent, p.KeepAlive)

		if err := c.write(func(w io.Writer) error {
			return mqtt.WriteConnack(w, sessionPresent, 0x00)
		}); err != nil {
			return err
		}

		for _, pub := range pendingPub {
			var publish *mqtt.Publish

			if pub.dup {
				pid := pub.packetID
				publish = &mqtt.Publish{
					TopicName: pub.topic,
					Payload:   pub.payload,
					QoS:       pub.qos,
					Retain:    pub.retain,
					Dup:       true,
					PacketID:  &pid,
				}
				b.forward(c.id, c, publish)
				continue
			}

			publish = buildOutgoingPublish(&mqtt.Publish{
				TopicName: pub.topic,
				Payload:   pub.payload,
				QoS:       pub.qos,
				Retain:    pub.retain,
			}, c, pub.qos, pub.retain, false)

			if pub.dup {
				pid := pub.packetID
				publish.PacketID = &pid
			}
			b.forward(c.id, c, publish)
		}
		return nil
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

func (s *session) enqueue(pub pendingPublish) {
	s.pendingPub = append(s.pendingPub, pub)
	if len(s.pendingPub) > maxPendingPerSession {
		s.pendingPub = s.pendingPub[len(s.pendingPub)-maxPendingPerSession:]
	}
}

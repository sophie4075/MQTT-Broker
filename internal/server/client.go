package broker

import (
	"BA-Broker/internal/mqtt"
	"io"
	"net"
	"sync"
	"time"
)

// Client represents a connected MQTT client.
type Client struct {
	conn         net.Conn
	id           string
	writeMu      sync.Mutex
	nextPktID    uint16
	pendingQoS2  map[uint16]struct{}
	subsMu       sync.Mutex
	subs         map[string]struct{}
	cleanSession bool
	will         *willMessage
	readTimeout  time.Duration
	outMu        sync.Mutex
	outPub       []pendingPublish
}

type willMessage struct {
	topic   string
	payload []byte
	qos     mqtt.QoS
	retain  bool
}

// write ensures only one goroutine writes to the connection at a time.
func (c *Client) write(fn func(io.Writer) error) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return fn(c.conn)
}

// allocPacketID returns the next server-assigned Packet Identifier for a
// QoS > 0 message to this client, independent of the client's own IDs.
func (c *Client) allocPacketID() uint16 {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.nextPktID++
	if c.nextPktID == 0 {
		c.nextPktID = 1
	}
	return c.nextPktID
}

// pendingQoS records packetID as a QoS 2 mid-delivery awaiting PUBREL.
func (c *Client) pendingQoS(packetID uint16) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.pendingQoS2 == nil {
		c.pendingQoS2 = make(map[uint16]struct{})
	}

	if _, exists := c.pendingQoS2[packetID]; exists {
		// a retransmission that must not be re-delivered
		return false
	}
	c.pendingQoS2[packetID] = struct{}{}
	return true
}

// clearPendingQoS removes packetID once its PUBREL has been handled.
func (c *Client) clearPendingQoS(packetID uint16) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	delete(c.pendingQoS2, packetID)
}

// markPubrecReceived marks a tracked outgoing QoS2 delivery as having had
// its PUBREC acknowledged (MQTT 4.4.0-1).
func (c *Client) markPubrecReceived(id uint16) {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	for i := range c.outPub {
		if c.outPub[i].packetID == id {
			c.outPub[i].pubrecReceived = true
			return
		}
	}
}

// addSub records that this client is now subscribed to filter
func (c *Client) addSub(filter string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if c.subs == nil {
		c.subs = make(map[string]struct{})
	}
	c.subs[filter] = struct{}{}
}

// removeSub records that this client is no longer subscribed to filter
func (c *Client) removeSub(filter string) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	delete(c.subs, filter)
}

// snapshotSubs returns a copy of this client's currently subscribed filters
// TODO Check if necessary
func (c *Client) snapshotSubs() map[string]struct{} {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	cp := make(map[string]struct{}, len(c.subs))
	for f := range c.subs {
		cp[f] = struct{}{}
	}
	return cp
}

func (c *Client) snapshotPub() []pendingPublish {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	out := c.outPub
	c.outPub = nil
	return out
}

func (c *Client) snapshotPktID() uint16 {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.nextPktID
}

func (c *Client) snapshotPendingQoS2() map[uint16]struct{} {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	out := c.pendingQoS2
	c.pendingQoS2 = nil
	return out
}

func (c *Client) trackPub(id uint16, pub pendingPublish) {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	pub.packetID = id
	c.outPub = append(c.outPub, pub)
}

func (c *Client) clearTracking(id uint16) {
	c.outMu.Lock()
	defer c.outMu.Unlock()
	for i, p := range c.outPub {
		if p.packetID == id {
			c.outPub = append(c.outPub[:i], c.outPub[i+1:]...)
			return
		}
	}
}

package broker

import (
	"io"
	"net"
	"sync"
)

// Client represents a connected MQTT client.
type Client struct {
	conn      net.Conn
	id        string
	writeMu   sync.Mutex
	nextPktID uint16
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

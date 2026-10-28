package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"time"

	"BA-Broker/internal/mqtt"
)

// Listener accepts TCP connections and serves each one in its own goroutine. The Go runtime's network poller
// multiplexes all these goroutines onto a small number of OS threads.
type Listener struct {
	Addr   string
	Server *Server
}

// connectTimeout bounds how long a connection may sit idle before sending
// its first CONNECT.
const connectTimeout = 5 * time.Second

// ListenAndServe binds to l.Addr and accepts connections until the listener is
// closed. Each accepted connection is handled in its own goroutine.
func (l *Listener) ListenAndServe() error {
	ln, err := net.Listen("tcp", l.Addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := ln.Close(); err != nil {
			log.Printf("mqtt listener close: %v", err)
		}
	}()
	log.Printf("mqtt listening on %s", l.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // listener deliberately closed -> clean stop
			}
			log.Printf("mqtt accept: %v", err)
			continue // a single failed accept must not kill the server
		}
		go l.handleConn(conn)
	}
}

func (l *Listener) handleConn(conn net.Conn) {
	addr := conn.RemoteAddr()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("recovered panic for %s: %v", addr, r)
		}
	}()

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("mqtt close %s: %v", addr, err)
		}
	}()
	log.Printf("mqtt connection from %s", addr)

	r := bufio.NewReader(conn)
	client := &Client{
		conn: conn,
	}

	cleanExit := false
	defer func() {
		if !cleanExit {
			l.Server.PublishWill(client)
		}
		l.Server.RemoveClient(client)
	}()

	connected := false
	for {
		if err := conn.SetReadDeadline(readDeadlineFor(connected, client.readTimeout)); err != nil {
			log.Printf("mqtt set read deadline for %s: %v", addr, err)
		}

		pkt, err := mqtt.ReadPacket(r)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				if !connected {
					log.Printf("mqtt timed out waiting for CONNECT from %s", addr)
					return
				}

				log.Printf("mqtt keep-alive timeout for %s", addr)
				return
			}
			if !errors.Is(err, io.EOF) {
				log.Printf("mqtt read error from %s: %v", addr, err)
			}
			return
		}

		if _, isConnect := pkt.(*mqtt.Connect); isConnect {
			if connected {
				log.Printf("protocol violation: second CONNECT from %s", addr)
				return // MQTT-3.1.0-2
			}
			connected = true
		} else if !connected {
			log.Printf("protocol violation: first packet from %s was not CONNECT", addr)
			return // MQTT-3.1.0-1
		}

		if err := l.Server.HandlePacket(client, pkt); err != nil {
			if errors.Is(err, errClientDisconnect) {
				cleanExit = true
				return
			}
			log.Printf("connection closed for %s: %v", addr, err)
			return
		}
	}
}

func readDeadlineFor(connected bool, keepAlive time.Duration) time.Time {
	if !connected {
		return time.Now().Add(connectTimeout)
	}
	if keepAlive == 0 {
		return time.Time{}
	}
	return time.Now().Add(keepAlive)
}

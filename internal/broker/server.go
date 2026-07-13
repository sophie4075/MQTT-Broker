package broker

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"

	"BA-Broker/internal/mqtt"
)

// Server accepts TCP connections and serves each one in its own goroutine. The Go runtime's network poller
// multiplexes all these goroutines onto a small number of OS threads.
type Server struct {
	Addr   string
	Broker *Broker
}

// ListenAndServe binds to s.Addr and accepts connections until the listener is
// closed. Each accepted connection is handled in its own goroutine.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := ln.Close(); err != nil {
			log.Printf("mqtt listener close: %v", err)
		}
	}()
	log.Printf("mqtt listening on %s", s.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil // listener deliberately closed -> clean stop
			}
			log.Printf("mqtt accept: %v", err)
			continue // a single failed accept must not kill the server
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	addr := conn.RemoteAddr()
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

	for {
		pkt, err := mqtt.ReadPacket(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("mqtt read error from %s: %v", addr, err)
			}
			return
		}

		if err := s.Broker.HandlePacket(client, pkt); err != nil {
			if !errors.Is(err, errClientDisconnect) {
				return
			}
			log.Printf("broker error for %s: %v", addr, err)
			return
		}
	}
}

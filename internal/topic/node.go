package topic

import "BA-Broker/internal/mqtt"

type node struct {
	children map[string]*node    // keyed by literal level, "+" , or "#"
	subs     map[string]mqtt.QoS // clientID -> QoS
}

// newNode returns a node with both maps initialized, ready to be written to.
func newNode() *node {
	return &node{
		children: make(map[string]*node),
		subs:     make(map[string]mqtt.QoS),
	}
}

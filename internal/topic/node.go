package topic

import (
	"BA-Broker/internal/mqtt"
	"strings"
)

type node struct {
	children map[string]*node    // keyed by literal level, "+" , or "#"
	subs     map[string]mqtt.QoS // clientID -> QoS
}

func (n *node) collect(parts []string, i int, results map[string]mqtt.QoS) {
	// every level of the topic name is consumed
	if i == len(parts) {
		mergeSubs(results, n.subs)
		return
	}
	level := parts[i]
	if child, ok := n.children[level]; ok {
		child.collect(parts, i+1, results)
	}

	if !(i == 0 && strings.HasPrefix(level, "$")) {
		if child, ok := n.children["+"]; ok {
			child.collect(parts, i+1, results)
		}

		if child, ok := n.children["#"]; ok {
			mergeSubs(results, child.subs)
		}
	}
}

// mergeSubs copies src into dst, keeping the higher QoS whenever a clientID
// already has an entry. See MQTT-3.3.5-1.
func mergeSubs(dst, src map[string]mqtt.QoS) {
	for id, qos := range src {
		if existing, ok := dst[id]; !ok || qos > existing {
			dst[id] = qos
		}
	}
}

// newNode returns a node with both maps initialized, ready to be written to.
func newNode() *node {
	return &node{
		children: make(map[string]*node),
		subs:     make(map[string]mqtt.QoS),
	}
}

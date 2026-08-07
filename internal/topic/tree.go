package topic

import (
	"strings"
	"sync"

	"BA-Broker/internal/mqtt"
)

type Tree struct {
	mu   sync.RWMutex
	root *node
}

func NewTree() *Tree {
	return &Tree{root: newNode()}
}

// Subscribe registers clientID's interest in filter at the given QoS,
// creating any missing nodes along the way.
func (t *Tree) Subscribe(filter string, clientID string, qos mqtt.QoS) error {
	if err := ValidateFilter(filter); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cur := t.root
	for _, level := range strings.Split(filter, "/") {
		child, ok := cur.children[level]
		if !ok {
			child = newNode()
			cur.children[level] = child
		}
		cur = child
	}
	cur.subs[clientID] = qos
	return nil
}

// Unsubscribe removes clientID's interest in filter, if present. Unlike
// Subscribe, it never creates nodes, if the path doesn't exist there's
// nothing to remove.
func (t *Tree) Unsubscribe(filter string, clientID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cur := t.root
	for _, level := range strings.Split(filter, "/") {
		child, ok := cur.children[level]
		if !ok {
			return
		}
		cur = child
	}
	delete(cur.subs, clientID)
}
func (t *Tree) Match(topicName string) map[string]mqtt.QoS {
	t.mu.RLock()
	defer t.mu.RUnlock()

	parts := strings.Split(topicName, "/")
	results := make(map[string]mqtt.QoS)
	t.root.collect(parts, 0, results)
	return results
}

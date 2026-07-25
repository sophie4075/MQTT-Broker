package topic

import "BA-Broker/internal/mqtt"

type node struct {
	children map[string]*node    // keyed by literal level, "+" , or "#"
	subs     map[string]mqtt.QoS // clientID -> QoS
}

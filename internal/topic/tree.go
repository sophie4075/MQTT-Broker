package topic

import "sync"

type Tree struct {
	mu   sync.RWMutex
	root *node
}

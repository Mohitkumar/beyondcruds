package raftmeta

import "sync"

type ClusterState struct {
	mu     sync.RWMutex
	Topics map[string]*TopicState
}

type TopicState struct {
	Name     string
	LeaderID string
	Epoch    uint64
	ISR      map[string]bool
}

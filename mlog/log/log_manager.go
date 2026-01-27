package log

import (
	"sync"
)

type LogManager struct {
	mu sync.RWMutex
	*Log
	leo           uint64 // Log End Offset
	highWatermark uint64
}

func NewLogManager(dir string) (*LogManager, error) {
	log, err := NewLog(dir)
	if err != nil {
		return nil, err
	}
	return &LogManager{
		Log: log,
	}, nil
}

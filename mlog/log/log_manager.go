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

func (l *LogManager) LEO() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leo
}

func (l *LogManager) SetLEO(leo uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.leo = leo
}

func (l *LogManager) HighWatermark() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.highWatermark
}

func (l *LogManager) SetHighWatermark(highWatermark uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.highWatermark = highWatermark
}

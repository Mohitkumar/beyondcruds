package log

import (
	"fmt"
	"sync"

	"github.com/mohitkumar/mlog/api/common"
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

	lm := &LogManager{
		Log: log,
	}

	// Initialize LEO from the active segment's NextOffset (restart scenario)
	// NextOffset is the next offset to write, which is exactly what LEO represents
	if log.activeSegment != nil {
		lm.leo = log.activeSegment.NextOffset
	} else {
		lm.leo = 0
	}

	return lm, nil
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

// Append appends a log entry and automatically advances LEO
func (l *LogManager) Append(entry *common.LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.Log.Append(entry)
	if err != nil {
		return 0, err
	}

	// LEO is the next offset to write, so after writing at offset N, LEO becomes N+1
	l.leo = offset + 1

	return offset, nil
}

// Read reads a log entry at the given offset, but only if it's within the high watermark
// This ensures consumers can only read committed data (data replicated to all ISR followers)
func (l *LogManager) Read(offset uint64) (*common.LogEntry, error) {
	l.mu.RLock()
	hw := l.highWatermark
	l.mu.RUnlock()
	// Consumers should only be able to read up to (and including) the high watermark
	if offset > hw {
		return nil, fmt.Errorf("offset %d is beyond high watermark %d (uncommitted data)", offset, hw)
	}

	return l.Log.Read(offset)
}

// ReadUncommitted reads a log entry at the given offset without checking the high watermark
// This is used for replication purposes where we need to read all data up to LEO, not just HW
func (l *LogManager) ReadUncommitted(offset uint64) (*common.LogEntry, error) {
	return l.Log.Read(offset)
}

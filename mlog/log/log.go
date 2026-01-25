package log

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/mohitkumar/mlog/api/log"
	"github.com/mohitkumar/mlog/segment"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	segments      []*segment.Segment
	activeSegment *segment.Segment
}

func NewLog(dir string) (*Log, error) {
	log := &Log{
		Dir:      dir,
		segments: make([]*segment.Segment, 0),
	}

	err := os.MkdirAll(dir, 0755)

	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return nil, err
	}

	dirEnt, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(dirEnt) > 0 {
		// Load existing segments
		var baseOffsets []uint64
		for _, entry := range dirEnt {
			var baseOffset uint64
			n, err := fmt.Sscanf(entry.Name(), "%020d.log", &baseOffset)
			if n == 1 && err == nil {
				baseOffsets = append(baseOffsets, baseOffset)
			}
		}
		sort.Slice(baseOffsets, func(i int, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		})
		for _, baseOffset := range baseOffsets {
			seg, err := segment.LoadExistingSegment(baseOffset, dir)
			if err != nil {
				return nil, err
			}
			log.segments = append(log.segments, seg)
		}
		log.activeSegment = log.segments[len(log.segments)-1]
		return log, nil
	}
	// No existing segments, create a new one
	activeSegment, err := segment.NewSegment(0, dir)
	if err != nil {
		return nil, err
	}
	log.segments = append(log.segments, activeSegment)
	log.activeSegment = activeSegment
	return log, nil
}

func (l *Log) Append(record *log.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record.Payload)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsFull() {
		l.activeSegment, err = segment.NewSegment(l.activeSegment.NextOffset, l.Dir)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, l.activeSegment)
	}
	return off, nil
}

func (l *Log) Read(offset uint64) (*log.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var targetSegment *segment.Segment
	for _, seg := range l.segments {
		if offset >= seg.BaseOffset && offset < seg.NextOffset {
			targetSegment = seg
			break
		}
	}
	if targetSegment == nil || offset < targetSegment.BaseOffset || offset >= targetSegment.NextOffset {
		return nil, fmt.Errorf("offset %d out of range", offset)
	}
	r, err := targetSegment.ReadAt(offset)
	if err != nil {
		return nil, err
	}
	return &log.Record{
		Offset:  r.Offset,
		Payload: r.Payload,
	}, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

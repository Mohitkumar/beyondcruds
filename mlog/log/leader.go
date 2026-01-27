package log

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mohitkumar/mlog/api/common"
	"github.com/mohitkumar/mlog/api/producer"
	"github.com/mohitkumar/mlog/broker"
)

type TopicLeader struct {
	mu        sync.RWMutex
	Topic     string
	Log       *LogManager
	broker    *broker.Broker
	NodeID    string
	followers map[string]*FollowerState
}

type FollowerState struct {
	NodeID        string
	LastFetchTime time.Time

	// LEO (Log End Offset) is the last offset the follower successfully fetched
	LastFetchedOffset uint64

	// CaughtUp indicates if the follower is in the "In-Sync Replica" (ISR) set
	IsISR bool
}

func (l *TopicLeader) HandleProduce(ctx context.Context, logEntry *common.LogEntry, acks producer.AckMode) (uint64, error) {
	offset, err := l.Log.Append(logEntry)
	if err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if acks == producer.AckMode_ACK_LEADER {
		return offset, nil
	} else if acks == producer.AckMode_ACK_ALL {
		err := l.waitForAllFollowersToCatchUp(ctx, offset)
		if err != nil {
			return 0, fmt.Errorf("failed to wait for all followers to catch up: %w", err)
		}
	}
	return offset, nil
}

func (l *TopicLeader) waitForAllFollowersToCatchUp(ctx context.Context, offset uint64) error {
	// Periodically check followers for ACK_ALL
	timeout := time.After(5 * time.Second) // configurable timeout
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		allCaughtUp := true
		l.mu.RLock()
		for _, follower := range l.followers {
			if follower.IsISR {
				// Follower must have replicated at least up to our offset
				if follower.LastFetchedOffset < offset {
					allCaughtUp = false
					break
				}
			}
		}
		l.mu.RUnlock()
		if allCaughtUp {
			break
		}
		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timeout before all followers caught up")
		}
	}
	return nil
}

// 2. Called by gRPC Fetch API (from Followers)
func (l *TopicLeader) HandleFetch(followerID string, offset uint64) ([]*common.LogEntry, uint64, error) {
	l.mu.Lock()
	// Track follower progress
	if _, ok := l.followers[followerID]; !ok {
		l.followers[followerID] = &FollowerState{}
	}
	l.followers[followerID].LastFetchedOffset = offset
	l.followers[followerID].LastFetchTime = time.Now()
	l.mu.Unlock()

	// Try to advance High Watermark based on new follower info
	l.maybeAdvanceHW()

	// Return messages and current HW
	msgs, err := l.Log.Read(offset)
	if err != nil {
		return nil, 0, err
	}
	return []*common.LogEntry{msgs}, l.Log.highWatermark, nil
}

func (l *TopicLeader) maybeAdvanceHW() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Logic: HW is the highest offset replicated to a majority.
	// For your simple setup (e.g., 1 Leader, 1 Follower):
	// HW = min(Leader.LEO, all Follower.LastOffset)
	minOffset := l.Log.leo
	for _, f := range l.followers {
		if f.LastFetchedOffset < minOffset {
			minOffset = f.LastFetchedOffset
		}
	}
	l.Log.highWatermark = minOffset
}

// MaybeAdvanceHW is a public wrapper for maybeAdvanceHW
func (l *TopicLeader) MaybeAdvanceHW() {
	l.maybeAdvanceHW()
}

// RecordLEORemote is called by replica brokers to record their LEO with the leader
func (l *TopicLeader) RecordLEORemote(replicaID string, leo uint64, leoTime time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.followers[replicaID].LastFetchedOffset = leo
	l.followers[replicaID].LastFetchTime = leoTime
	return nil
}

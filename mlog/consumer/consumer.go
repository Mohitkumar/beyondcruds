package consumer

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/mohitkumar/mlog/log"
)

type ConsumerManager struct {
	mu          sync.RWMutex
	offsetCache map[string]map[string]uint64
	offsetLog   *log.Log
}

func NewConsumerManager(baseDir string) (*ConsumerManager, error) {
	offsetLog, err := log.NewLog(filepath.Join(baseDir, "__consumer_offsets__.log"))
	if err != nil {
		return nil, err
	}
	return &ConsumerManager{
		offsetCache: make(map[string]map[string]uint64),
		offsetLog:   offsetLog,
	}, nil
}

func (c *ConsumerManager) CommitOffset(id string, topic string, offset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.offsetCache[id]; !ok {
		c.offsetCache[id] = make(map[string]uint64)
	}
	c.offsetCache[id][topic] = offset
	_, err := c.offsetLog.AppendRaw([]byte(fmt.Sprintf("%s,%s,%d", id, topic, offset)))
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsumerManager) GetOffset(id string, topic string) (uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	topicMap, ok := c.offsetCache[id]
	if !ok {
		return 0, fmt.Errorf("offset not found for id: %s and topic: %s", id, topic)
	}
	return topicMap[topic], nil
}

func (c *ConsumerManager) Recover() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	offset := c.offsetLog.LowestOffset()

	highestOffset := c.offsetLog.HighestOffset()
	for ; offset < highestOffset; offset++ {
		data, err := c.offsetLog.ReadRaw(offset)
		if err != nil {
			return err
		}
		parts := strings.Split(string(data), ",")
		if len(parts) != 3 {
			continue
		}
		off, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}
		if _, ok := c.offsetCache[parts[0]]; !ok {
			c.offsetCache[parts[0]] = make(map[string]uint64)
		}
		c.offsetCache[parts[0]][parts[1]] = off
	}
	return nil
}

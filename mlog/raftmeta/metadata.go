package broker

import (
	"sync"

	"github.com/mohitkumar/mlog/api/metadata"
)

type PartitionMetadata struct {
	PartitionID string
	LeaderID    string
	LeaderEpoch int64
	ISR         map[string]bool
	HW          int64
}

type MetadataStore struct {
	mu         sync.RWMutex
	partitions map[string]*PartitionMetadata
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		partitions: make(map[string]*PartitionMetadata),
	}
}

func (ms *MetadataStore) GetPartitionMetadata(PartitionID string) (*PartitionMetadata, bool) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	partition, exists := ms.partitions[PartitionID]
	return partition, exists
}

func (ms *MetadataStore) Apply(ev *metadata.MetadataEvent) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	switch e := ev.Event.(type) {
	case *metadata.MetadataEvent_CreatePartition:
		replicas := make(map[string]bool)
		for _, r := range e.CreatePartition.Replicas {
			replicas[r] = true
		}
		ms.partitions[e.CreatePartition.PartitionId] = &PartitionMetadata{
			PartitionID: e.CreatePartition.PartitionId,
			ISR:         replicas,
			HW:          -1,
		}
	case *metadata.MetadataEvent_LeaderChange:
		p := ms.partitions[e.LeaderChange.PartitionId]
		if p != nil {
			p.LeaderID = e.LeaderChange.LeaderId
			p.LeaderEpoch = e.LeaderChange.LeaderEpoch
		}
	case *metadata.MetadataEvent_IsrUpdate:
		p := ms.partitions[e.IsrUpdate.PartitionId]
		if p != nil {
			newISR := make(map[string]bool)
			for _, r := range e.IsrUpdate.Isr {
				newISR[r] = true
			}
			p.ISR = newISR
		}
	case *metadata.MetadataEvent_HwUpdate:
		p := ms.partitions[e.HwUpdate.PartitionId]
		p.HW = e.HwUpdate.Offset
	}
}

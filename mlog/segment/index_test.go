package segment

import (
	"os"
	"testing"
)

func setupTest(t *testing.T) (index *Index, teardown func()) {
	t.Helper()

	index, err := OpenIndex("/tmp/test.idx")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	return index, func() {
		index.Close()
		os.Remove("/tmp/test.idx")
	}
}

func TestIndexWriteRead(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	entries := []struct {
		relOffset uint32
		position  uint64
	}{
		{0, 0},
		{1, 100},
		{2, 205},
		{3, 309},
		{4, 400},
	}

	for _, e := range entries {
		if err := index.Write(e.relOffset, e.position); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	for _, e := range entries {
		entry, found := index.Find(e.relOffset)
		if !found {
			t.Fatalf("failed to read index entry: %v", entry)
		}
		if entry.RelativeOffset != e.relOffset || entry.Position != e.position {
			t.Errorf("index entry mismatch: got (%d, %d), want (%d, %d)",
				entry.RelativeOffset, entry.Position, e.relOffset, e.position)
		}
	}
}

func TestIndexGrow(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	initialCap := len(index.mmap)

	entriesToWrite := (initialCap / IndexEntrySize) + 10 // Write more than initial capacity

	for i := 0; i < entriesToWrite; i++ {
		if err := index.Write(uint32(i), uint64(i*100)); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	if len(index.mmap) <= initialCap {
		t.Errorf("index mmap did not grow as expected")
	}

	// Verify last entry
	lastEntry, found := index.Find(uint32(entriesToWrite - 1))
	if !found {
		t.Fatalf("failed to read last index entry")
	}
	if lastEntry.RelativeOffset != uint32(entriesToWrite-1) || lastEntry.Position != uint64((entriesToWrite-1)*100) {
		t.Errorf("last index entry mismatch	: got (%d, %d), want (%d, %d)",
			lastEntry.RelativeOffset, lastEntry.Position, entriesToWrite-1, (entriesToWrite-1)*100)
	}
}

func TestIndexEntry(t *testing.T) {
	index, teardown := setupTest(t)
	defer teardown()

	entries := []struct {
		relOffset uint32
		position  uint64
	}{
		{0, 0},
		{1, 100},
		{2, 205},
	}

	for _, e := range entries {
		if err := index.Write(e.relOffset, e.position); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	for i, e := range entries {
		entry := index.Entry(int64(i))
		if entry.RelativeOffset != e.relOffset || entry.Position != e.position {
			t.Errorf("index entry mismatch: got (%d, %d), want (%d, %d)",
				entry.RelativeOffset, entry.Position, e.relOffset, e.position)
		}
	}
}

package segment

import (
	"fmt"
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
		fmt.Printf("Index Entry: %v", entry)
		if !found {
			t.Fatalf("failed to read index entry: %v", entry)
		}
		if entry.RelativeOffset != e.relOffset || entry.Position != e.position {
			t.Errorf("index entry mismatch: got (%d, %d), want (%d, %d)",
				entry.RelativeOffset, entry.Position, e.relOffset, e.position)
		}
	}
}

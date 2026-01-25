package segment

import (
	"os"
	"strconv"
	"testing"
)

func setupTestSegment(t *testing.T) (*Segment, func()) {
	t.Helper()

	segment, err := NewSegment(0, "/tmp")
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	return segment, func() {
		segment.Close()
		os.RemoveAll("/tmp/00000000000000000000.log")
		os.RemoveAll("/tmp/00000000000000000000.idx")
	}
}

func cleanup(t *testing.T) {
	t.Helper()
	os.RemoveAll("/tmp/00000000000000000000.log")
	os.RemoveAll("/tmp/00000000000000000000.idx")
}
func TestSegmentReadWrite(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	var offsets []uint64
	for _, r := range records {
		offset, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		offsets = append(offsets, offset)
	}

	for i, r := range records {
		rec, err := segment.ReadAt(offsets[i])
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Payload) != string(r) {
			t.Errorf("record mismatch: got %s, want %s", rec.Payload, r)
		}
	}
}

func TestSegmentReadWriteLarge(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		value := []byte("record number " + strconv.Itoa(i))
		_, err := segment.Append(value)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	for i := 0; i < numRecords; i++ {
		expectedValue := []byte("record number " + strconv.Itoa(i))
		rec, err := segment.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if string(rec.Payload) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Payload, expectedValue)
		}
	}
}

func TestSegmentOutOfRangeRead(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	_, err := segment.ReadAt(0)
	if err == nil {
		t.Fatalf("expected error for out-of-range read, got nil")
	}

	_, err = segment.Append([]byte("only record"))
	if err != nil {
		t.Fatalf("failed to append record: %v", err)
	}

	_, err = segment.ReadAt(2)
	if err == nil {
		t.Fatalf("expected error for out-of-range read, got nil")
	}
}

func TestLoadExistingSegment(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	records := [][]byte{
		[]byte("first record"),
		[]byte("second record"),
		[]byte("third record"),
	}

	for _, r := range records {
		_, err := segment.Append(r)
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}
	loadedSegment, err := LoadExistingSegment(0, "/tmp")
	if err != nil {
		t.Fatalf("failed to load existing segment: %v", err)
	}
	defer loadedSegment.Close()

	for i, r := range records {
		rec, err := loadedSegment.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Payload) != string(r) {
			t.Errorf("record mismatch: got value: %s), want value: %s)",
				rec.Payload, r)
		}
	}
}

func TestLoadExistingSegmentLarge(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		value := []byte("record number " + strconv.Itoa(i))
		_, err := segment.Append(value)
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	if err := segment.Close(); err != nil {
		t.Fatalf("failed to close segment: %v", err)
	}
	loadedSegment, err := LoadExistingSegment(0, "/tmp")
	if err != nil {
		t.Fatalf("failed to load existing segment: %v", err)
	}
	defer loadedSegment.Close()

	for i := 0; i < numRecords; i++ {
		expectedValue := []byte("record number " + strconv.Itoa(i))
		rec, err := loadedSegment.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if string(rec.Payload) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Payload, expectedValue)
		}
	}
}

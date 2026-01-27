package segment

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
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
		rec, err := segment.Read(offsets[i])
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got %s, want %s", rec.Value, r)
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
		rec, err := segment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if string(rec.Value) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Value, expectedValue)
		}
	}
}

func TestSegmentOutOfRangeRead(t *testing.T) {
	cleanup(t)
	segment, teardown := setupTestSegment(t)
	defer teardown()

	_, err := segment.Read(0)
	if err == nil {
		t.Fatalf("expected error for out-of-range read, got nil")
	}

	_, err = segment.Append([]byte("only record"))
	if err != nil {
		t.Fatalf("failed to append record: %v", err)
	}

	_, err = segment.Read(2)
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
		rec, err := loadedSegment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got value: %s), want value: %s)",
				rec.Value, r)
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
		rec, err := loadedSegment.Read(uint64(i))
		if err != nil {
			t.Fatalf("failed to read record %d: %v", i, err)
		}
		if string(rec.Value) != string(expectedValue) {
			t.Errorf("record %d mismatch: got %s, want %s", i, rec.Value, expectedValue)
		}
	}
}

func TestSegmentReader(t *testing.T) {
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
	reader := segment.Reader()
	var readRecords []string
	for i := 0; i < len(records); i++ {
		buf := make([]byte, 4+4)
		n, err := reader.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 8, n)
		size := binary.BigEndian.Uint32(buf[4:8])

		payloadBuf := make([]byte, size)
		n, err = reader.Read(payloadBuf)
		fmt.Println(string(payloadBuf))

		require.NoError(t, err)
		require.Equal(t, int(size), n)
		readRecords = append(readRecords, string(payloadBuf[8:]))
	}
	for i, r := range records {
		if string(r) != readRecords[i] {
			t.Errorf("record mismatch: got %s, want %s", readRecords[i], r)
		}
	}
}

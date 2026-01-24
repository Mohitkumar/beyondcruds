package log

import (
	"os"
	"strconv"
	"testing"

	"github.com/mohitkumar/mlog/api"
)

func setupTestLog(t *testing.T) (*Log, func()) {
	t.Helper()

	log, err := NewLog("/tmp/mlog_test")
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	return log, func() {
		log.Close()
		os.RemoveAll("/tmp/mlog_test")
	}
}

func TestLogAppendRead(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	records := []struct {
		timestamp uint64
		value     []byte
	}{
		{1625152800, []byte("first log record")},
		{1625152860, []byte("second log record")},
		{1625152920, []byte("third log record")},
	}

	var offsets []uint64
	for _, r := range records {
		offset, err := log.Append(&api.Record{
			Timestamp: r.timestamp,
			Value:     r.value,
		})
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		offsets = append(offsets, offset)
	}

	for i, r := range records {
		rec, err := log.Read(offsets[i])
		if err != nil {
			t.Fatalf("failed to read record: %v", err)
		}
		if rec.Timestamp != r.timestamp || string(rec.Value) != string(r.value) {
			t.Errorf("record mismatch: got (ts: %d, value: %s), want (ts: %d, value: %s)",
				rec.Timestamp, rec.Value, r.timestamp, r.value)
		}
	}
}

func TestLogOutOfRangeRead(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	_, err := log.Read(999)
	if err == nil {
		t.Fatalf("expected error for out of range read, got nil")
	}
}
func TestLogSegmentRotation(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	numRecords := 100000
	var lastOffset uint64
	for i := 0; i < numRecords; i++ {
		offset, err := log.Append(&api.Record{
			Timestamp: uint64(1625152800 + i*60),
			Value:     []byte("log record " + strconv.Itoa(i)),
		})
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
		lastOffset = offset
	}

	if len(log.segments) < 2 {
		t.Errorf("expected multiple segments after appending records, got %d", len(log.segments))
	}

	rec, err := log.Read(lastOffset)
	if err != nil {
		t.Fatalf("failed to read last record: %v", err)
	}
	expectedValue := "log record " + strconv.Itoa(numRecords-1)
	if string(rec.Value) != expectedValue {
		t.Errorf("last record value mismatch: got %s, want %s", rec.Value, expectedValue)
	}
}

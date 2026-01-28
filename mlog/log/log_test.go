package log

import (
	"os"
	"strconv"
	"testing"

	"github.com/mohitkumar/mlog/api/common"
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

	records := [][]byte{
		[]byte("first log record"),
		[]byte("second log record"),
		[]byte("third log record"),
	}

	var offsets []uint64
	for _, r := range records {
		offset, err := log.Append(&common.LogEntry{
			Value: r,
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
		if string(rec.Value) != string(r) {
			t.Errorf("record mismatch: got (payload: %s), want (payload: %s)",
				rec.Value, r)
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
		offset, err := log.Append(&common.LogEntry{
			Value: []byte("log record " + strconv.Itoa(i)),
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
		t.Errorf("last record payload mismatch: got %s, want %s", rec.Value, expectedValue)
	}
}

func TestLogReader(t *testing.T) {
	log, teardown := setupTestLog(t)
	defer teardown()

	records := [][]byte{
		[]byte("first log record"),
		[]byte("second log record"),
		[]byte("third log record"),
	}

	for _, r := range records {
		_, err := log.Append(&common.LogEntry{
			Value: r,
		})
		if err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	reader := log.Reader()
	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("failed to read from log reader: %v", err)
	}

	readData := buf[:n]
	for _, r := range records {
		if !containsRecord(readData, r) {
			t.Errorf("log reader data missing record: %s", r)
		}
	}
}

func containsRecord(data []byte, record []byte) bool {
	return string(data) != "" && string(record) != "" && (len(data) >= len(record)) && (string(data[:len(record)]) == string(record) || containsRecord(data[1:], record))
}

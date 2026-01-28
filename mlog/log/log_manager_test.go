package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mohitkumar/mlog/api/common"
)

func TestLogManager_Read_WithHighWatermark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "test-log")
	lm, err := NewLogManager(dir)
	if err != nil {
		t.Fatalf("NewLogManager error: %v", err)
	}
	defer os.RemoveAll(dir)

	// Append some entries
	entry1 := &common.LogEntry{Value: []byte("message-1")}
	offset1, err := lm.Append(entry1)
	if err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if offset1 != 0 {
		t.Fatalf("expected offset 0, got %d", offset1)
	}

	entry2 := &common.LogEntry{Value: []byte("message-2")}
	offset2, err := lm.Append(entry2)
	if err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if offset2 != 1 {
		t.Fatalf("expected offset 1, got %d", offset2)
	}

	entry3 := &common.LogEntry{Value: []byte("message-3")}
	offset3, err := lm.Append(entry3)
	if err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if offset3 != 2 {
		t.Fatalf("expected offset 2, got %d", offset3)
	}

	// Set high watermark to 1 (only first two messages are committed)
	lm.SetHighWatermark(1)

	// Should be able to read offset 0 (within HW)
	readEntry1, err := lm.Read(0)
	if err != nil {
		t.Fatalf("Read(0) error: %v", err)
	}
	if string(readEntry1.Value) != "message-1" {
		t.Fatalf("expected 'message-1', got '%s'", string(readEntry1.Value))
	}

	// Should be able to read offset 1 (at HW)
	readEntry2, err := lm.Read(1)
	if err != nil {
		t.Fatalf("Read(1) error: %v", err)
	}
	if string(readEntry2.Value) != "message-2" {
		t.Fatalf("expected 'message-2', got '%s'", string(readEntry2.Value))
	}

	// Should NOT be able to read offset 2 (beyond HW)
	_, err = lm.Read(2)
	if err == nil {
		t.Fatalf("expected error when reading beyond high watermark, got nil")
	}
	expectedError := "offset 2 is beyond high watermark 1 (uncommitted data)"
	if err.Error() != expectedError {
		t.Fatalf("expected error '%s', got '%s'", expectedError, err.Error())
	}

	// Advance high watermark to 2
	lm.SetHighWatermark(2)

	// Now should be able to read offset 2
	readEntry3, err := lm.Read(2)
	if err != nil {
		t.Fatalf("Read(2) error after HW advance: %v", err)
	}
	if string(readEntry3.Value) != "message-3" {
		t.Fatalf("expected 'message-3', got '%s'", string(readEntry3.Value))
	}
}

package segment

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/mohitkumar/mlog/api/common"
)

const (
	IndexIntervalBytes = 4 * 1024    // 4KB
	MaxSegmentBytes    = 1024 * 1024 // 1MB
)

// Segement represents a log segment consisting of a log file and an index file.
// Base offset is the starting offset of the segment, NextOffset is the next offset to be assigned
// Each log file is named as {baseOffset}.log and each index file is named as {baseOffset}.idx
type Segment struct {
	BaseOffset          uint64 //base offset of the segment
	NextOffset          uint64 //next offset of the segment
	MaxOffset           uint64 //max offset of the segment
	logFile             *os.File
	bufWriter           *bufio.Writer // buffered writer for log file
	index               *Index
	bytesSinceLastIndex uint64
	writePos            int64      // current end-of-log position
	mu                  sync.Mutex // protects writes
}

func NewSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := dir + "/" + formatLogFileName(baseOffset)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	indexFilePath := dir + "/" + formatIndexFileName(baseOffset)
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	return &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		bufWriter:  bufio.NewWriterSize(logFile, 64*1024), // 64KB buffer
		index:      index,
		writePos:   0,
	}, nil
}

func LoadExistingSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := dir + "/" + formatLogFileName(baseOffset)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	indexFilePath := dir + "/" + formatIndexFileName(baseOffset)
	index, err := OpenIndex(indexFilePath)
	if err != nil {
		logFile.Close()
		return nil, err
	}
	segment := &Segment{
		BaseOffset: baseOffset,
		NextOffset: baseOffset,
		logFile:    logFile,
		bufWriter:  bufio.NewWriterSize(logFile, 64*1024), // 64KB buffer
		index:      index,
	}
	if err := segment.Recover(); err != nil {
		logFile.Close()
		index.Close()
		return nil, err
	}
	return segment, nil
}

func formatLogFileName(baseOffset uint64) string {
	return fmt.Sprintf("%020d.log", baseOffset)
}

func formatIndexFileName(baseOffset uint64) string {
	return fmt.Sprintf("%020d.idx", baseOffset)
}

func (s *Segment) Append(value []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := s.NextOffset
	record := common.NewLogEntry(offset, value)
	pos := s.writePos

	// Use buffered writer (no Seek needed - it writes sequentially)
	n, err := record.Encode(s.bufWriter)
	if err != nil {
		return 0, err
	}
	s.writePos += int64(n)

	s.bytesSinceLastIndex += n
	if s.bytesSinceLastIndex >= IndexIntervalBytes || offset == s.BaseOffset {
		// Flush buffer before indexing to ensure data is written
		if err := s.bufWriter.Flush(); err != nil {
			return 0, err
		}
		if err := s.index.Write(uint32(offset-s.BaseOffset), uint64(pos)); err != nil {
			return 0, err
		}
		s.bytesSinceLastIndex = 0
	}
	s.NextOffset++
	s.MaxOffset = offset
	return offset, nil
}

func (s *Segment) Read(offset uint64) (*common.LogEntry, error) {
	s.mu.Lock()
	// Flush any pending writes before reading
	if s.bufWriter != nil {
		if err := s.bufWriter.Flush(); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()

	if offset < s.BaseOffset || offset >= s.NextOffset {
		return nil, fmt.Errorf("offset %d out of range [%d, %d)", offset, s.BaseOffset, s.NextOffset)
	}

	relOffset := uint32(offset - s.BaseOffset)
	indexEntry, found := s.index.Find(relOffset)
	if !found {
		return nil, fmt.Errorf("index entry not found for relative offset %d", relOffset)
	}

	if _, err := s.logFile.Seek(int64(indexEntry.Position), io.SeekStart); err != nil {
		return nil, err
	}
	for {
		rec, _, err := common.DecodeLogEntry(s.logFile)
		if err != nil {
			return nil, err
		}
		if rec.Offset == offset {
			return rec, nil
		}
	}
}

func (s *Segment) ReadAt(buf []byte, offset int64) (int, error) {
	return s.logFile.ReadAt(buf, offset)
}

func (s *Segment) Recover() error {
	var (
		startPos   int64 = 0
		nextOffset       = s.BaseOffset
	)

	if last, ok := s.index.Last(); ok {
		startPos = int64(last.Position)
		nextOffset = s.BaseOffset + uint64(last.RelativeOffset)
	}

	if _, err := s.logFile.Seek(startPos, io.SeekStart); err != nil {
		return err
	}

	pos := startPos
	offset := nextOffset

	for {
		rec, size, err := common.DecodeLogEntry(s.logFile)
		if err != nil {
			break
		}

		if rec.Offset != offset {
			break
		}

		pos += int64(size)
		offset++
	}

	// truncate log at last good position
	if err := s.logFile.Truncate(pos); err != nil {
		return err
	}
	// Keep our fast-path end position in sync with recovered/truncated log.
	s.writePos = pos
	// Reset buffered writer to start from recovered position
	if s.bufWriter != nil {
		s.bufWriter.Reset(s.logFile)
	}

	// truncate index if it points past log
	s.index.TruncateAfter(uint64(pos))

	s.NextOffset = offset
	s.MaxOffset = offset - 1
	return nil
}

func (s *Segment) IsFull() bool {
	info, err := s.logFile.Stat()
	if err != nil {
		return false
	}
	if info.Size() >= MaxSegmentBytes {
		return true
	}
	return false
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.bufWriter != nil {
		if err := s.bufWriter.Flush(); err != nil {
			return err
		}
	}
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Reader() io.Reader {
	s.mu.Lock()
	// Flush any pending writes before creating reader
	if s.bufWriter != nil {
		_ = s.bufWriter.Flush()
	}
	s.mu.Unlock()

	s.logFile.Seek(0, io.SeekStart)
	return s.logFile
}

type segmentReader struct {
	*Segment
	pos int64
}

func (sr *segmentReader) Read(p []byte) (n int, err error) {
	n, err = sr.ReadAt(p, sr.pos)
	sr.pos += int64(n)
	return n, err
}

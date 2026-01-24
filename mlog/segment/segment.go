package segment

import (
	"fmt"
	"io"
	"os"
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
	index               *Index
	bytesSinceLastIndex uint64
}

func NewSegment(baseOffset uint64, dir string) (*Segment, error) {
	logFilePath := dir + "/" + formatLogFileName(baseOffset)
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
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
		index:      index,
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

func (s *Segment) Append(ts uint64, value []byte) (uint64, error) {
	pos, err := s.logFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	offset := s.NextOffset
	record := NewRecord(offset, ts, value)
	n, err := record.Encode(s.logFile)
	if err != nil {
		return 0, err
	}

	s.bytesSinceLastIndex += n
	if s.bytesSinceLastIndex >= IndexIntervalBytes || offset == s.BaseOffset {
		if err := s.index.Write(uint32(offset-s.BaseOffset), uint64(pos)); err != nil {
			return 0, err
		}
		s.bytesSinceLastIndex = 0
	}
	s.NextOffset++
	s.MaxOffset = offset
	return offset, nil
}

func (s *Segment) ReadAt(offset uint64) (*Record, error) {
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
		rec, _, err := DecodeRecord(s.logFile)
		if err != nil {
			return nil, err
		}
		if rec.Offset == offset {
			return rec, nil
		}
	}
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
		rec, size, err := DecodeRecord(s.logFile)
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

	// truncate index if it points past log
	s.index.TruncateAfter(uint32(pos))

	s.NextOffset = offset
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
	if err := s.logFile.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}

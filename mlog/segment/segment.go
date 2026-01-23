package segment

import (
	"fmt"
	"io"
	"os"
)

const (
	IndexIntervalBytes = 4 * 1024 // 4KB
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
	if s.bytesSinceLastIndex >= IndexIntervalBytes {
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
	indexEntry, err := s.index.Read(relOffset)
	if err != nil {
		return nil, err
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
	var startPos uint64 = 0
	if s.index.Size() > 0 {
		last, err := s.index.Read(uint32(s.index.Size() - IndexEntrySize))
		if err != nil {
			return err
		}
		startPos = last.Position
		s.NextOffset = s.BaseOffset + uint64(last.RelOffset)
	}

	pos := startPos
	for {
		_, err := s.logFile.Seek(int64(pos), io.SeekStart)
		if err != nil {
			break
		}

		rec, n, err := DecodeRecord(s.logFile)
		if err != nil {
			_ = s.logFile.Truncate(int64(pos))
			break
		}

		pos += n
		s.NextOffset = rec.Offset + 1
	}

	return nil
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

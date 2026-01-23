package segment

import (
	"io"

	"github.com/go-mmap/mmap"
)

// IndexEntry represents an entry in the index file. Its a sparse index, we only index once segment reachs 4KB.
// its physical layout in the index file is as follows:
// +----------------+----------------+
// |   RelOffset    |    Position    |
// +----------------+----------------+
// |    4 bytes     |    8 bytes     |
// +----------------+----------------+

const (
	IndexEntrySize = 4 + 8 // relOffset + position

)

type IndexEntry struct {
	RelOffset uint32 //relative offset to the base offset of the segment
	Position  uint64 //position of the record in the log file
}

func NewIndexEntry(relOffset uint32, position uint64) *IndexEntry {
	return &IndexEntry{
		RelOffset: relOffset,
		Position:  position,
	}
}

func (ie *IndexEntry) Encode(w io.Writer) error {
	buf := make([]byte, IndexEntrySize)
	endian.PutUint32(buf[0:4], ie.RelOffset)
	endian.PutUint64(buf[4:12], ie.Position)
	_, err := w.Write(buf)
	return err
}

func DecodeIndexEntry(data []byte) *IndexEntry {
	relOffset := endian.Uint32(data[0:4])
	position := endian.Uint64(data[4:12])
	return &IndexEntry{
		RelOffset: relOffset,
		Position:  position,
	}
}

// Index represents the index file for a log segment.
// Example [0,101],[100,205],[200,309],[300,410]...
type Index struct {
	indexFile *mmap.File
}

func OpenIndex(filePath string) (*Index, error) {
	mmapFile, err := mmap.OpenFile(filePath, mmap.Read|mmap.Write)
	if err != nil {
		return nil, err
	}
	return &Index{
		indexFile: mmapFile,
	}, nil
}

func (idx *Index) Write(relOffset uint32, position uint64) error {
	indexEntry := NewIndexEntry(relOffset, position)
	if err := indexEntry.Encode(idx.indexFile); err != nil {
		return err
	}
	return nil
}

func (idx *Index) Read(relOffset uint32) (*IndexEntry, error) {
	var entry *IndexEntry

	low, high := 0, idx.entryCount()-1
	for low <= high {
		mid := (low + high) / 2
		midOffset := mid * IndexEntrySize
		buf := make([]byte, IndexEntrySize)
		_, err := idx.indexFile.ReadAt(buf, int64(midOffset))
		if err != nil {
			return nil, err
		}
		e := DecodeIndexEntry(buf)
		if e.RelOffset <= relOffset {
			entry = e
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return entry, nil
}

func (idx *Index) entryCount() int {
	return int(idx.indexFile.Len() / IndexEntrySize)
}

func (idx *Index) Size() int64 {
	info, err := idx.indexFile.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func (idx *Index) Close() error {
	return idx.indexFile.Close()
}

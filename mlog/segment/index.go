package segment

import (
	"encoding/binary"
	"os"

	"github.com/tysonmote/gommap"
)

// IndexEntry represents an entry in the index file. Its a sparse index, we only index once segment reachs 4KB.
// its physical layout in the index file is as follows:
// +----------------+----------------+
// |   RelOffset    |    Position    |
// +----------------+----------------+
// |    4 bytes     |    8 bytes     |
// +----------------+----------------+

const (
	IndexEntrySize = 4 + 8    // relOffset + position
	IndexSize      = 4 * 1024 //4KB
)

var indexEndian = binary.BigEndian

type IndexEntry struct {
	RelativeOffset uint32
	Position       uint64
}

// Index represents the index file for a log segment.
// Example [0,101],[100,205],[200,309],[300,410]...
type Index struct {
	file *os.File
	mmap gommap.MMap
	size int64
}

func OpenIndex(filePath string) (*Index, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		if err := file.Truncate(IndexSize); err != nil {
			return nil, err
		}
	}
	m, err := gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// Use stat.Size() as the initial size - it should reflect actual written data.
	// If the file was just created and truncated to IndexSize, stat.Size() will be IndexSize (capacity),
	// but we'll start with size=0 and grow as we write. For existing files with data, stat.Size()
	// should be accurate.
	initialSize := stat.Size()
	// If file was pre-allocated (truncated to IndexSize) but empty, start at 0
	if initialSize == IndexSize {
		// Check if first entry is actually written (non-zero or first entry at 0,0)
		firstBuf := m[0:IndexEntrySize]
		firstRel := indexEndian.Uint32(firstBuf[0:4])
		firstPos := indexEndian.Uint64(firstBuf[4:12])
		if firstRel == 0 && firstPos == 0 {
			// Might be empty pre-allocated file, scan to find actual usage
			initialSize = 0
			for pos := int64(0); pos+IndexEntrySize <= int64(len(m)); pos += IndexEntrySize {
				buf := m[pos : pos+IndexEntrySize]
				rel := indexEndian.Uint32(buf[0:4])
				p := indexEndian.Uint64(buf[4:12])
				// Stop at first empty entry (but first (0,0) is valid)
				if pos > 0 && rel == 0 && p == 0 {
					break
				}
				initialSize += IndexEntrySize
			}
		}
	}

	idx := &Index{
		file: file,
		mmap: m,
		size: initialSize,
	}

	return idx, nil
}

func (idx *Index) Write(relOffset uint32, position uint64) error {
	if idx.size+IndexEntrySize > int64(len(idx.mmap)) {
		if err := idx.grow(); err != nil {
			return err
		}
	}

	buf := idx.mmap[idx.size : idx.size+IndexEntrySize]
	indexEndian.PutUint32(buf[0:4], relOffset)
	indexEndian.PutUint64(buf[4:12], position)

	idx.size += IndexEntrySize
	return nil
}

func (idx *Index) grow() error {
	newSize := int64(len(idx.mmap)) * 2

	if err := idx.mmap.UnsafeUnmap(); err != nil {
		return err
	}

	if err := idx.file.Truncate(newSize); err != nil {
		return err
	}

	m, err := gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return err
	}

	idx.mmap = m

	return nil
}

func (idx *Index) Entry(i int64) IndexEntry {
	pos := i * IndexEntrySize
	buf := idx.mmap[pos : pos+IndexEntrySize]

	return IndexEntry{
		RelativeOffset: indexEndian.Uint32(buf[0:4]),
		Position:       indexEndian.Uint64(buf[4:12]),
	}
}

func (idx *Index) Find(relOffset uint32) (IndexEntry, bool) {
	count := idx.size / IndexEntrySize
	if count == 0 {
		return IndexEntry{}, false
	}

	var result IndexEntry
	low, high := int64(0), count-1

	for low <= high {
		mid := (low + high) / 2
		entry := idx.Entry(mid)

		if entry.RelativeOffset <= relOffset {
			result = entry
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return result, true
}

func (idx *Index) Last() (IndexEntry, bool) {
	count := idx.size / IndexEntrySize
	if count == 0 {
		return IndexEntry{}, false
	}
	return idx.Entry(count - 1), true
}

func (idx *Index) TruncateAfter(position uint64) error {
	var truncateSize int64 = idx.size
	count := idx.size / IndexEntrySize

	for i := count - 1; i >= 0; i-- {
		entry := idx.Entry(i)
		if entry.Position <= position {
			break
		}
		truncateSize -= IndexEntrySize
	}

	if truncateSize == idx.size {
		return nil
	}
	idx.size = truncateSize
	return idx.file.Truncate(idx.size)
}

func (idx *Index) Size() int64 {
	return idx.size
}

func (idx *Index) Close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}
	return idx.file.Close()
}

package common

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync"
)

const (
	RecordHeaderSize = 4 + 4 + 8 // crc + size + offset
)

var endian = binary.BigEndian

var encodeBufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024) // Pre-allocated capacity for small records
	},
}

// Record represents a log record with an offset, and value.
// Its physical layout in the log file is as follows:
// +----------------+----------------+----------------|----------------+
// |      CRC       |     Size       |    Offset      |    Value       |
// +----------------+----------------+----------------+----------------+
// |    4 bytes     |    4 bytes     |    8 bytes     |   Size bytes   |
// +----------------+----------------+----------------+----------------+
func NewLogEntry(offset uint64, value []byte) *LogEntry {
	return &LogEntry{
		Offset: offset,
		Value:  value,
	}
}

func (r *LogEntry) Encode(w io.Writer) (uint64, error) {
	size := RecordHeaderSize + len(r.Value)
	payloadSize := uint32(8 + len(r.Value)) // offset (8 bytes) + value

	// Reuse buffer from pool to avoid allocations
	buf := encodeBufPool.Get().([]byte)
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	defer encodeBufPool.Put(buf[:0]) // Reset length but keep capacity

	endian.PutUint32(buf[4:8], payloadSize)
	endian.PutUint64(buf[8:16], uint64(r.Offset))
	copy(buf[16:], r.Value)
	crc := crc32.ChecksumIEEE(buf[8:])
	endian.PutUint32(buf[0:4], crc)
	n, err := w.Write(buf)
	return uint64(n), err
}

func DecodeLogEntry(r io.Reader) (*LogEntry, uint64, error) {
	header := make([]byte, 8)

	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}
	crc := endian.Uint32(header[0:4])
	size := endian.Uint32(header[4:8])

	// Validate size: must be at least 8 bytes (for offset) + payload
	// Size field represents: 8 bytes (offset) + value length
	if size < 8 {
		return nil, 0, io.ErrUnexpectedEOF
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 0, err
	}
	if len(data) < 8 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	if crc32.ChecksumIEEE(data) != crc {
		return nil, 0, io.ErrUnexpectedEOF
	}

	return &LogEntry{
		Offset: endian.Uint64(data[0:8]),
		Value:  data[8:],
	}, uint64(8 + size), nil
}

package common

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	RecordHeaderSize = 4 + 4 + 8 // crc + size + offset
)

var endian = binary.BigEndian

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
	buf := make([]byte, size)
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

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 0, err
	}
	if crc32.ChecksumIEEE(data) != crc {
		return nil, 0, io.ErrUnexpectedEOF
	}

	return &LogEntry{
		Offset: endian.Uint64(data[0:8]),
		Value:  data[8:],
	}, uint64(8 + size), nil
}

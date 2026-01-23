package segment

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	RecordHeaderSize = 4 + 4 + 8 + 8 // crc + size + offset + timestamp
)

var endian = binary.BigEndian

// Record represents a log record with an offset, timestamp, and value.
// Its physical layout in the log file is as follows:
// +----------------+----------------+----------------+----------------+----------------+
// |      CRC       |     Size       |    Offset      |   Timestamp    |     Value      |
// +----------------+----------------+----------------+----------------+----------------+
// |    4 bytes     |    4 bytes     |    8 bytes     |    8 bytes     |   Size bytes   |
// +----------------+----------------+----------------+----------------+----------------+
type Record struct {
	Offset    uint64
	Timestamp uint64
	Value     []byte
}

func NewRecord(offset uint64, timestamp uint64, value []byte) *Record {
	return &Record{
		Offset:    offset,
		Timestamp: timestamp,
		Value:     value,
	}
}

func (r *Record) Encode(w io.Writer) (uint64, error) {
	size := RecordHeaderSize + len(r.Value)
	payloadSize := uint32(8 + 8 + len(r.Value))
	buf := make([]byte, size)
	endian.PutUint32(buf[4:8], payloadSize)
	endian.PutUint64(buf[8:16], uint64(r.Offset))
	endian.PutUint64(buf[16:24], uint64(r.Timestamp))
	copy(buf[24:], r.Value)
	crc := crc32.ChecksumIEEE(buf[8:])
	endian.PutUint32(buf[0:4], crc)
	n, err := w.Write(buf)
	return uint64(n), err
}

func DecodeRecord(r io.Reader) (*Record, uint64, error) {
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

	return &Record{
		Offset:    endian.Uint64(data[0:8]),
		Timestamp: endian.Uint64(data[8:16]),
		Value:     data[16:],
	}, uint64(8 + size), nil
}

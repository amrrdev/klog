package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	TypeWrite  uint8 = 1
	TypeDelete uint8 = 2
	TypeCommit uint8 = 3

	// LSN(8) + Type(1) + CRC32(4) + PayloadLength(4) = 17
	headerSize = 17
)

// castagnoli is computed once at init time and reused across all checksum calls.
// CRC32 with this polynomial runs at memory bandwidth speed on x86 via SSE4.2.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Record struct {
	LSN     uint64
	Type    uint8
	Payload []byte
}

// encode serializes r into the binary wire format.
// It allocates exactly headerSize + len(r.Payload) bytes.
func encode(r Record) []byte {
	buf := make([]byte, headerSize+len(r.Payload))

	binary.LittleEndian.PutUint64(buf[0:8], r.LSN)
	buf[8] = r.Type
	binary.LittleEndian.PutUint32(buf[9:13], crc32.Checksum(r.Payload, castagnoli))
	binary.LittleEndian.PutUint32(buf[13:17], uint32(len(r.Payload)))
	copy(buf[17:], r.Payload)

	return buf
}

func decode(r io.Reader) (Record, error) {
	headers := make([]byte, headerSize)
	_, err := io.ReadFull(r, headers)
	if err != nil {
		// io.EOF here means the reader was empty — clean end of log.
		// io.ErrUnexpectedEOF means we read some bytes but not a full header
		// — this is a partial write, treat it as corruption.
		if err == io.EOF {
			return Record{}, io.EOF
		}
		if err == io.ErrUnexpectedEOF {
			return Record{}, fmt.Errorf("partial header (corrupt tail): %w", err)
		}

		return Record{}, fmt.Errorf("truncated headers: %w", err)
	}

	rec := Record{
		LSN:  binary.LittleEndian.Uint64(headers[0:8]),
		Type: headers[8],
	}
	storedChecksum := binary.LittleEndian.Uint32(headers[9:13])
	payloadLen := binary.LittleEndian.Uint32(headers[13:17])

	rec.Payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(r, rec.Payload); err != nil {
		return Record{}, fmt.Errorf("truncated payload at LSN %d: %w", rec.LSN, err)
	}

	// Verify integrity. A mismatch here almost always means the process
	// crashed between writing the header and writing the full payload,
	// leaving a partial record at the tail of the log.
	computed := crc32.Checksum(rec.Payload, castagnoli)
	if computed != storedChecksum {
		return Record{}, fmt.Errorf("checksum mismatch at LSN %d: stored=%d computed=%d",
			rec.LSN, storedChecksum, computed)
	}

	return rec, nil
}

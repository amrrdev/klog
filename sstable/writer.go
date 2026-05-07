package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/amrrdev/wal/memtable"
)

const (
	// magic is written at the end of every SSTable file.
	// On open, we verify this value to detect truncated or corrupt files.
	magic = uint64(0xDEADC0FFEE)

	// IndexOffset(8) + BloomOffset(8) + Magic(8) = 24 bytes.
	footerSize        = 24
	falsePositiveRate = 0.01
)

// indexEntry records the key and its byte offset in the data block section.
// The full index is held in memory during writing and flushed after all data.
type indexEntry struct {
	key    string
	offset uint64
}

type Writer struct {
	file   *os.File
	buf    *bufio.Writer
	index  []indexEntry
	bloom  *bloomFilter
	offset uint64
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("create sstable %q: %w", path, err)
	}
	return &Writer{
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
	}, nil
}

// Flush writes all entries from the memtable to the SSTable file.
// entries must be sorted in ascending key order — memtable.Iter() guarantees this.
// After Flush returns, the file is complete and durable on disk.
func (w *Writer) Flush(entries []memtable.Entry) error {
	if len(entries) == 0 {
		return fmt.Errorf("cannot flush empty memtable")
	}

	w.bloom = newBloomFilter(len(entries), falsePositiveRate)

	// Phase 1: write all data blocks sequentially.
	for _, entry := range entries {
		if err := w.writeEntry(entry); err != nil {
			return fmt.Errorf("write entry %q: %w", entry.Key, err)
		}
	}

	// Phase 2: write the index block.
	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return fmt.Errorf("write index: %w", err)
	}

	// Phase 3: write the bloom filter.
	bloomOffset := w.offset
	if err := w.writeBloom(); err != nil {
		return fmt.Errorf("write bloom: %w", err)
	}

	// Phase 4: write the footer.
	if err := w.writeFooter(indexOffset, bloomOffset); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}

	return w.file.Close()
}

// writeEntry encodes one key-value entry into the data block format and
// writes it to the file. It records the entry's offset in the index and
// adds the key to the bloom filter.
func (w *Writer) writeEntry(entry memtable.Entry) error {
	key := []byte(entry.Key)
	value := entry.Value

	var deleted uint8
	if entry.Deleted {
		deleted = 1
		value = nil
	}

	w.index = append(w.index, indexEntry{
		key:    entry.Key,
		offset: w.offset,
	})

	w.bloom.add(key)

	header := make([]byte, 9)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
	header[8] = deleted

	if _, err := w.buf.Write(header); err != nil {
		return err
	}
	if _, err := w.buf.Write(key); err != nil {
		return err
	}
	if _, err := w.buf.Write(value); err != nil {
		return err
	}

	w.offset += uint64(9 + len(key) + len(value))
	return nil
}

// writeIndex encodes the index block and writes it to the file.
// Each index entry: [KeyLen:4][Offset:8][Key]
func (w *Writer) writeIndex() error {
	for _, entry := range w.index {
		key := []byte(entry.key)
		buf := make([]byte, 12+len(key))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
		binary.LittleEndian.PutUint64(buf[4:12], entry.offset)
		copy(buf[12:], key)

		if _, err := w.buf.Write(buf); err != nil {
			return err
		}
		w.offset += uint64(len(buf))
	}
	return nil
}

func (w *Writer) writeBloom() error {
	encoded := w.bloom.encode()
	if _, err := w.buf.Write(encoded); err != nil {
		return err
	}
	w.offset += uint64(len(encoded))
	return nil
}

func (w *Writer) writeFooter(indexOffset, bloomOffset uint64) error {
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomOffset)
	binary.LittleEndian.PutUint64(footer[16:24], magic)

	_, err := w.buf.Write(footer)
	return err
}

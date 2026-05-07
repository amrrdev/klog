package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type Reader struct {
	file  *os.File
	index []indexEntry
	bloom *bloomFilter
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable %q: %w", path, err)
	}

	r := &Reader{file: f}
	if err := r.readFooterAndLoad(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) Get(key string) (value []byte, deleted bool, err error) {
	if !r.bloom.mayContain([]byte(key)) {
		return nil, false, nil
	}

	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].key >= key
	})

	if idx >= len(r.index) || r.index[idx].key != key {
		// Key is not in this SSTable — bloom filter gave a false positive.
		return nil, false, nil
	}

	return r.readEntryAt(r.index[idx].offset)

}

func (r *Reader) readFooterAndLoad() error {
	info, err := r.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < footerSize {
		return fmt.Errorf("file too small to be a valid sstable")
	}

	footer := make([]byte, footerSize)
	if _, err := r.file.ReadAt(footer, info.Size()-footerSize); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footer[8:16])
	fileMagic := binary.LittleEndian.Uint64(footer[16:24])

	if fileMagic != magic {
		return fmt.Errorf("invalid magic number: file is corrupt or not an sstable")
	}

	indexData := make([]byte, bloomOffset-indexOffset)
	if _, err := r.file.ReadAt(indexData, int64(indexOffset)); err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	r.index = decodeIndex(indexData)

	bloomSize := uint64(info.Size()) - footerSize - bloomOffset
	bloomData := make([]byte, bloomSize)
	if _, err := r.file.ReadAt(bloomData, int64(bloomOffset)); err != nil {
		return fmt.Errorf("read bloom filter: %w", err)
	}
	r.bloom = decodeBloomFilter(bloomData)

	return nil
}

// decodeIndex parses the raw index bytes into a slice of indexEntry.
// Each entry: [KeyLen:4][Offset:8][Key]
func decodeIndex(data []byte) []indexEntry {
	var entries []indexEntry
	pos := 0
	for pos < len(data) {
		keyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		offset := binary.LittleEndian.Uint64(data[pos+4 : pos+12])
		key := string(data[pos+12 : pos+12+keyLen])
		entries = append(entries, indexEntry{key: key, offset: offset})
		pos += 12 + keyLen
	}
	return entries
}

// readEntryAt seeks to the given byte offset and reads one data block entry.
// Entry format: [KeyLen:4][ValueLen:4][Deleted:1][Key][Value]
func (r *Reader) readEntryAt(offset uint64) (value []byte, deleted bool, err error) {
	header := make([]byte, 9)
	if _, err := r.file.ReadAt(header, int64(offset)); err != nil {
		return nil, false, fmt.Errorf("read entry header at offset %d: %w", offset, err)
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	valuelen := binary.LittleEndian.Uint32(header[4:8])
	isDeleted := header[8] == 1

	payload := make([]byte, keyLen+valuelen)
	if _, err := r.file.ReadAt(payload, int64(offset)+9); err != nil {
		if err == io.EOF && uint32(len(payload)) == keyLen+valuelen {
			// ReadAt on the last entry may return EOF alongside the data.
			// This is valid and treat it as a successful read.
		} else {
			return nil, false, fmt.Errorf("read entry payload: %w", err)
		}
	}
	if isDeleted {
		return nil, true, nil
	}
	return payload[keyLen:], false, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

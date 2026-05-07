package sstable

import (
	"fmt"
	"path/filepath"

	"github.com/amrrdev/wal/memtable"
)

// Flush writes the memtable contents to a new SSTable file.
// The filename encodes the sequence number so SSTables can be ordered
// from newest to oldest during reads — newer SSTables shadow older ones.
//
// seqNum must be monotonically increasing across all flushes.
// It is the caller's responsibility to track and increment seqNum.
func Flush(dir string, seqNum uint64, mem *memtable.Memtable) (*Reader, error) {
	entries := mem.Iter()
	if len(entries) == 0 {
		return nil, fmt.Errorf("nothing to flush")
	}

	path := filepath.Join(dir, fmt.Sprintf("%016d.sst", seqNum))

	w, err := NewWriter(path)
	if err != nil {
		return nil, err
	}

	if err := w.Flush(entries); err != nil {
		return nil, fmt.Errorf("flush to %q: %w", path, err)
	}

	// Open the newly written SSTable immediately so the caller can
	// add it to the active reader list without a separate Open call.
	return Open(path)
}

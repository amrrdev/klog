package wal

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// maxSegmentSize is the maximum byte size of a single log segment file
	// before rotation. 64MB matches PostgreSQL's default wal_segment_size.
	maxSegmentSize = 64 * 1024 * 1024

	// bufferSize is the size of the write buffer in front of the log file.
	// Writes smaller than this are buffered in memory and flushed together,
	// reducing the number of write() syscalls. The buffer is always flushed
	// before fsync — fsync on an unflushed buffer only syncs what the kernel
	// has seen, not what is still in userspace memory.
	bufferSize = 4 * 1024 // 4KB
)

type WAL struct {
	mu      sync.Mutex
	dir     string
	file    *os.File      // active segment, open for appending
	buf     *bufio.Writer // userspace buffer in front of file
	nextLSN uint64        // byte offset where the next record will be written
	size    int64         // current byte size of the active segment
}

func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	wal := &WAL{dir: dir}
	segments, err := wal.sortSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		return wal, wal.openNewSegment()
	}

	// Existing segments found — reopen the last one for appending.
	// initLSN computes nextLSN from the last segment's name + its file size,
	// which gives the correct global byte offset for the next record.
	if err := wal.initLSN(segments); err != nil {
		return nil, err
	}

	return wal, wal.reopenSegment(segments[len(segments)-1])
}

func (w *WAL) sortSegments() ([]string, error) {
	segments, err := filepath.Glob(filepath.Join(w.dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(segments)
	return segments, nil
}

func (w *WAL) initLSN(segments []string) error {
	last := segments[len(segments)-1]

	base := strings.TrimSuffix(filepath.Base(last), ".log")
	startLSN, err := strconv.ParseUint(base, 16, 64)
	if err != nil {
		return fmt.Errorf("invalid segment filename %q: %w", last, err)
	}

	info, err := os.Stat(last)
	if err != nil {
		return fmt.Errorf("stat segment %q: %w:", last, err)
	}

	w.nextLSN = startLSN + uint64(info.Size())
	return nil
}

func (w *WAL) openNewSegment() error {
	name := filepath.Join(w.dir, fmt.Sprintf("%016x.log", w.nextLSN))
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("create segment %q: %w", name, err)
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, bufferSize)
	w.size = 0
	return nil
}

func (w *WAL) reopenSegment(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("reopen segment %q: %q", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("stat segment %q: %w", path, err)
	}

	w.file = f
	w.size = info.Size()
	w.buf = bufio.NewWriterSize(f, bufferSize)
	return nil
}

// Write appends a record to the log and syncs it to stable storage before
// returning. The returned LSN is the byte offset of this record in the
// global log stream — it can be used to seek directly to this record.
func (w *WAL) Write(recordType uint8, payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lsn := w.nextLSN

	encoded := encode(Record{LSN: w.nextLSN, Type: recordType, Payload: payload})
	if _, err := w.buf.Write(encoded); err != nil {
		return 0, fmt.Errorf("file write: %w", err)
	}
	if err := w.buf.Flush(); err != nil {
		return 0, fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("fsync: %w", err)
	}

	size := int64(len(encoded))
	w.nextLSN += uint64(size)
	w.size += size

	if w.size >= maxSegmentSize {
		if err := w.rotate(); err != nil {
			return 0, fmt.Errorf("rotate: %w", err)
		}
	}

	return lsn, nil
}

// rotate closes the active segment and opens a new one.
// Must be called with w.mu held.
func (w *WAL) rotate() error {
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close segment: %w", err)
	}

	return w.openNewSegment()
}

// Recover replays all segments in LSN order, calling fn for each valid record.
// It stops at the first corrupt record at the tail of the last segment —
// this indicates a partial write from a crash. The corrupt tail is truncated.
// Records in earlier segments are never truncated: a full segment that was
// rotated away is assumed to be complete and correct.
//
// Recover must be called before any writes if existing segments are present.
// It is safe to call on a fresh WAL with no segments — fn is never called.
func (w *WAL) Recover(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.sortSegments()
	if err != nil {
		return err
	}
	for i, seg := range segments {
		isLast := i == len(segments)-1
		if err := w.recoverSegment(seg, isLast, fn); err != nil {
			return err
		}
	}
	return nil
}

// recoverSegment replays one segment file.
// If isLast is true, a corrupt record at the tail triggers truncation.
// If isLast is false (fully rotated segment), corruption is a hard error —
// a complete segment should never have a corrupt tail.
func (w *WAL) recoverSegment(path string, isLast bool, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var lastGoodOffset int64

	for {
		rec, err := decode(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Corruption in a completed segment is unexpected and unrecoverable.
			if isLast {
				return fmt.Errorf("corrupt record in completed segment %s at offset %d: %w",
					filepath.Base(path), lastGoodOffset, err)
			}
			// Corrupt tail of the active segment — partial write from a crash.
			// Truncate at the last known-good offset and stop.
			log.Printf("WAL: corrupt tail in %s at offset %d — truncating",
				filepath.Base(path), lastGoodOffset)

			if err := os.Truncate(path, lastGoodOffset); err != nil {
				return fmt.Errorf("truncate corrupted tail: %w", err)
			}

			base := strings.TrimSuffix(filepath.Base(path), ".log")
			startLSN, err := strconv.ParseUint(base, 16, 64)
			if err != nil {
				return fmt.Errorf("invalid segment filename: %w", err)
			}
			w.nextLSN = startLSN + uint64(lastGoodOffset)
			w.size = lastGoodOffset
			break
		}
		if err := fn(rec); err != nil {
			return fmt.Errorf("recovery handler at LSN %d: %w", rec.LSN, err)
		}
		lastGoodOffset += int64(headerSize + len(rec.Payload))
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("flush on close: %w", err)
	}
	return w.file.Close()
}

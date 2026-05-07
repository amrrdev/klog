package sstable

import (
	"os"
	"testing"

	"github.com/amrrdev/wal/memtable"
)

func TestNewWriter(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.file.Close()

	if w == nil {
		t.Fatal("Writer should not be nil")
	}
	if w.offset != 0 {
		t.Errorf("expected offset 0, got %d", w.offset)
	}
}

func TestWriter_FlushEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer w.file.Close()

	err = w.Flush([]memtable.Entry{})
	if err == nil {
		t.Error("expected error for empty entries")
	}
}

func TestWriter_FlushSingleEntry(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "key1", Value: []byte("value1")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("file should not be empty")
	}
}

func TestWriter_FlushMultipleEntries(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("22")},
		{Key: "c", Value: []byte("333")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Error("file should not be empty")
	}
}

func TestWriter_FlushWithDeletedEntry(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Deleted: true},
		{Key: "key3", Value: []byte("value3")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestWriter_FlushSortedEntries(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "z", Value: []byte("1")},
		{Key: "a", Value: []byte("2")},
		{Key: "m", Value: []byte("3")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestWriter_FlushEmptyValue(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "key1", Value: []byte("")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestWriter_FlushLargeValues(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	largeValue := make([]byte, 1024*1024)
	entries := []memtable.Entry{
		{Key: "key1", Value: largeValue},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() < 1024*1024 {
		t.Errorf("file size should be at least 1MB, got %d", info.Size())
	}
}

func TestWriter_FlushCreatesIndex(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer r.Close()

	if len(r.index) != 2 {
		t.Errorf("expected 2 index entries, got %d", len(r.index))
	}
}

func TestWriter_FlushCreatesBloom(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
	}

	err = w.Flush(entries)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer r.Close()

	if r.bloom == nil {
		t.Error("bloom filter should not be nil")
	}
}
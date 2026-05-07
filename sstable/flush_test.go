package sstable

import (
	"path/filepath"
	"testing"

	"github.com/amrrdev/wal/memtable"
)

func TestFlush(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte("value1"))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	val, deleted, err := r.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted {
		t.Error("key1 should not be deleted")
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got %s", string(val))
	}
}

func TestFlush_EmptyMemtable(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()

	_, err := Flush(tmpDir, 1, m)
	if err == nil {
		t.Error("expected error for empty memtable")
	}
}

func TestFlush_FileCreated(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte("value1"))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 sst file, got %d", len(files))
	}
}

func TestFlush_FilenameEncoding(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte("value1"))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	r.Close()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}

	expected := "0000000000000001.sst"
	if filepath.Base(files[0]) != expected {
		t.Errorf("expected %s, got %s", expected, filepath.Base(files[0]))
	}
}

func TestFlush_SequentialSeqNum(t *testing.T) {
	tmpDir := t.TempDir()

	m1 := memtable.New()
	m1.Set("key1", []byte("value1"))
	r1, err := Flush(tmpDir, 1, m1)
	if err != nil {
		t.Fatalf("Flush 1 failed: %v", err)
	}
	r1.Close()

	m2 := memtable.New()
	m2.Set("key2", []byte("value2"))
	r2, err := Flush(tmpDir, 2, m2)
	if err != nil {
		t.Fatalf("Flush 2 failed: %v", err)
	}
	defer r2.Close()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("expected 2 sst files, got %d", len(files))
	}
}

func TestFlush_WithDeletedEntries(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte("value1"))
	m.Delete("key2")
	m.Set("key3", []byte("value3"))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	val, deleted, err := r.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted || string(val) != "value1" {
		t.Error("key1 should have value1")
	}

	_, deleted, _ = r.Get("key2")
	if !deleted {
		t.Error("key2 should be deleted")
	}

	val, deleted, err = r.Get("key3")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted || string(val) != "value3" {
		t.Error("key3 should have value3")
	}
}

func TestFlush_EmptyValue(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte(""))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	val, deleted, err := r.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted {
		t.Error("key1 should not be deleted")
	}
	if len(val) != 0 {
		t.Errorf("expected empty value, got %d bytes", len(val))
	}
}

func TestFlush_MultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	for i := 0; i < 26; i++ {
		key := string(rune('a' + i))
		value := string(rune('0' + i))
		m.Set(key, []byte(value))
	}

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	for i := 0; i < 26; i++ {
		key := string(rune('a' + i))
		expected := string(rune('0' + i))
		val, _, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", key, err)
		}
		if string(val) != expected {
			t.Errorf("key %s: expected %s, got %s", key, expected, string(val))
		}
	}
}

func TestFlush_ReturnsReader(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	m.Set("key1", []byte("value1"))

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	val, _, err := r.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got %s", string(val))
	}
}

func TestFlush_CheckFileSize(t *testing.T) {
	tmpDir := t.TempDir()

	m := memtable.New()
	largeValue := make([]byte, 1024*1024)
	m.Set("key1", largeValue)

	r, err := Flush(tmpDir, 1, m)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	defer r.Close()

	info, err := r.file.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() < 1024*1024 {
		t.Errorf("file should be at least 1MB, got %d", info.Size())
	}
}
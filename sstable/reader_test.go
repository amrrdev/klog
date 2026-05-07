package sstable

import (
	"testing"

	"github.com/amrrdev/wal/memtable"
)

func TestReader_Get(t *testing.T) {
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

func TestReader_GetSecondKey(t *testing.T) {
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

	val, deleted, err := r.Get("key2")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted {
		t.Error("key2 should not be deleted")
	}
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got %s", string(val))
	}
}

func TestReader_GetNotFound(t *testing.T) {
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

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer r.Close()

	val, deleted, err := r.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if deleted {
		t.Error("nonexistent should not be marked as deleted")
	}
	if val != nil {
		t.Error("value should be nil for nonexistent key")
	}
}

func TestReader_GetDeletedKey(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Deleted: true},
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

	val, deleted, err := r.Get("key2")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !deleted {
		t.Error("key2 should be marked as deleted")
	}
	if val != nil {
		t.Error("value should be nil for deleted key")
	}
}

func TestReader_GetEmptyValue(t *testing.T) {
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

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
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

func TestReader_Close(t *testing.T) {
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

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = r.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestReader_GetMultiple(t *testing.T) {
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
		{Key: "d", Value: []byte("4444")},
		{Key: "e", Value: []byte("55555")},
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

	tests := []struct {
		key    string
		expect string
	}{
		{"a", "1"},
		{"b", "22"},
		{"c", "333"},
		{"d", "4444"},
		{"e", "55555"},
	}

	for _, tc := range tests {
		val, deleted, err := r.Get(tc.key)
		if err != nil {
			t.Fatalf("Get failed for %s: %v", tc.key, err)
		}
		if deleted {
			t.Errorf("key %s should not be deleted", tc.key)
		}
		if string(val) != tc.expect {
			t.Errorf("key %s: expected %s, got %s", tc.key, tc.expect, string(val))
		}
	}
}

func TestReader_GetSortedOrder(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test.sst"

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	entries := []memtable.Entry{
		{Key: "a", Value: []byte("2")},
		{Key: "m", Value: []byte("3")},
		{Key: "z", Value: []byte("1")},
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

	val, _, _ := r.Get("a")
	if string(val) != "2" {
		t.Errorf("expected '2', got %s", string(val))
	}

	val, _, _ = r.Get("m")
	if string(val) != "3" {
		t.Errorf("expected '3', got %s", string(val))
	}

	val, _, _ = r.Get("z")
	if string(val) != "1" {
		t.Errorf("expected '1', got %s", string(val))
	}
}

func TestReader_BloomFilter(t *testing.T) {
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

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer r.Close()

	if r.bloom == nil {
		t.Error("bloom filter should not be nil")
	}

	if !r.bloom.mayContain([]byte("key1")) {
		t.Error("key1 should be in bloom filter")
	}
}
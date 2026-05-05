package memtable

import (
	"testing"
)

func TestMemtable_New(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("New() returned nil")
	}
	if m.sl == nil {
		t.Error("skip list should not be nil")
	}
	if m.MaxSize == 0 {
		t.Error("MaxSize should not be zero")
	}
}

func TestMemtable_Set(t *testing.T) {
	m := New()
	m.Set("key1", []byte("value1"))

	val, ok := m.Get("key1")
	if !ok {
		t.Error("expected key1 to exist")
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got %s", string(val))
	}
}

func TestMemtable_Get(t *testing.T) {
	m := New()

	val, ok := m.Get("nonexistent")
	if ok {
		t.Error("expected no value for nonexistent key")
	}
	if val != nil {
		t.Error("expected nil value")
	}
}

func TestMemtable_Delete(t *testing.T) {
	m := New()
	m.Set("key1", []byte("value1"))

	val, ok := m.Get("key1")
	if !ok {
		t.Fatal("key1 should exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected 'value1', got %s", string(val))
	}

	m.Delete("key1")

	val, ok = m.Get("key1")
	if ok {
		t.Error("key1 should not exist after delete")
	}
}

func TestMemtable_DeleteNonExistent(t *testing.T) {
	m := New()
	m.Delete("nonexistent")
}

func TestMemtable_Size(t *testing.T) {
	m := New()
	if m.Size() != 0 {
		t.Errorf("expected size 0, got %d", m.Size())
	}

	m.Set("key", []byte("value"))
	if m.Size() <= 0 {
		t.Error("size should be positive after set")
	}
}

func TestMemtable_Iter(t *testing.T) {
	m := New()
	entries := m.Iter()
	if len(entries) != 0 {
		t.Errorf("expected empty iter, got %d entries", len(entries))
	}

	m.Set("b", []byte("2"))
	m.Set("a", []byte("1"))
	m.Set("c", []byte("3"))

	entries = m.Iter()
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Key != "a" || entries[1].Key != "b" || entries[2].Key != "c" {
		t.Error("entries should be in sorted order")
	}
}

func TestMemtable_Overwrite(t *testing.T) {
	m := New()
	m.Set("key1", []byte("value1"))

	val, ok := m.Get("key1")
	if !ok {
		t.Fatal("key should exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected 'value1', got %s", string(val))
	}

	m.Set("key1", []byte("value2"))

	val, ok = m.Get("key1")
	if !ok {
		t.Fatal("key should still exist")
	}
	if string(val) != "value2" {
		t.Fatalf("expected 'value2', got %s", string(val))
	}
}

func TestMemtable_DeleteThenSet(t *testing.T) {
	m := New()
	m.Set("key1", []byte("value1"))
	m.Delete("key1")
	m.Set("key1", []byte("value2"))

	val, ok := m.Get("key1")
	if !ok {
		t.Error("key1 should exist after delete then set")
	}
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got %s", string(val))
	}
}

func TestMemtable_ShouldFlush(t *testing.T) {
	m := New()
	if m.ShouldFlush() {
		t.Error("empty memtable should not flush")
	}

	m.Set("key", []byte("value"))
	if m.ShouldFlush() {
		t.Error("small memtable should not flush")
	}
}

func TestMemtable_MultipleKeys(t *testing.T) {
	m := New()
	keys := []string{"z", "a", "m", "b", "k"}
	values := []string{"5", "1", "4", "2", "3"}

	for i := range keys {
		m.Set(keys[i], []byte(values[i]))
	}

	for i := range keys {
		val, ok := m.Get(keys[i])
		if !ok {
			t.Errorf("key %s should exist", keys[i])
		}
		if string(val) != values[i] {
			t.Errorf("key %s: expected %s, got %s", keys[i], values[i], string(val))
		}
	}

	entries := m.Iter()
	if len(entries) != len(keys) {
		t.Errorf("expected %d entries, got %d", len(keys), len(entries))
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Key < entries[i-1].Key {
			t.Error("entries should be in sorted order")
		}
	}
}

func TestMemtable_DeleteAll(t *testing.T) {
	m := New()
	m.Set("a", []byte("1"))
	m.Set("b", []byte("2"))
	m.Set("c", []byte("3"))

	m.Delete("a")
	m.Delete("b")
	m.Delete("c")

	entries := m.Iter()
	if len(entries) != 3 {
		t.Errorf("expected 3 entries (with tombstones), got %d", len(entries))
	}

	for _, e := range entries {
		if !e.Deleted {
			t.Error("all entries should be deleted")
		}
	}
}

func TestMemtable_EmptyValue(t *testing.T) {
	m := New()
	m.Set("key", []byte(""))

	val, ok := m.Get("key")
	if !ok {
		t.Error("key should exist")
	}
	if len(val) != 0 {
		t.Errorf("expected empty value, got %d bytes", len(val))
	}
}

func TestMemtable_SizeAfterDelete(t *testing.T) {
	m := New()
	m.Set("key", []byte("value"))
	initialSize := m.Size()

	m.Delete("key")

	if m.Size() >= initialSize {
		t.Error("size should decrease after delete")
	}
}

func TestMemtable_Concurrent(t *testing.T) {
	m := New()

	for i := 0; i < 10; i++ {
		go func(idx int) {
			m.Set(string(rune('a'+idx)), []byte(string(rune('0'+idx))))
		}(i)
	}

	_, _ = m.Get("a")
}
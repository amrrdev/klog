package memtable

import (
	"strconv"
	"testing"
)

func TestSkipList_New(t *testing.T) {
	sl := NewSkipList()
	if sl == nil {
		t.Fatal("NewSkipList() returned nil")
	}
	if sl.head == nil {
		t.Error("head should not be nil")
	}
	if sl.level != 0 {
		t.Errorf("expected level 0, got %d", sl.level)
	}
}

func TestSkipList_Set(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))

	val, ok := sl.Get("key1")
	if !ok {
		t.Error("expected key1 to exist")
	}
	if string(val) != "value1" {
		t.Errorf("expected 'value1', got %s", string(val))
	}
}

func TestSkipList_SetMultiple(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))
	sl.Set("key2", []byte("value2"))
	sl.Set("key3", []byte("value3"))

	val, ok := sl.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if string(val) != "value1" {
		t.Errorf("key1: expected 'value1', got %s", string(val))
	}

	val, ok = sl.Get("key2")
	if !ok {
		t.Error("key2 should exist")
	}
	if string(val) != "value2" {
		t.Errorf("key2: expected 'value2', got %s", string(val))
	}

	val, ok = sl.Get("key3")
	if !ok {
		t.Error("key3 should exist")
	}
	if string(val) != "value3" {
		t.Errorf("key3: expected 'value3', got %s", string(val))
	}
}

func TestSkipList_SetUpdatesValue(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))

	val, ok := sl.Get("key1")
	if !ok {
		t.Fatal("key should exist")
	}
	if string(val) != "value1" {
		t.Fatalf("expected 'value1', got %s", string(val))
	}

	sl.Set("key1", []byte("newvalue"))

	val, ok = sl.Get("key1")
	if !ok {
		t.Fatal("key should still exist")
	}
	if string(val) != "newvalue" {
		t.Fatalf("expected 'newvalue', got %s", string(val))
	}
}

func TestSkipList_Get(t *testing.T) {
	sl := NewSkipList()

	val, ok := sl.Get("nonexistent")
	if ok {
		t.Error("should not find nonexistent key")
	}
	if val != nil {
		t.Error("value should be nil")
	}
}

func TestSkipList_Delete(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))

	_, ok := sl.Get("key1")
	if !ok {
		t.Fatal("key should exist")
	}

	sl.Delete("key1")

	val, ok := sl.Get("key1")
	if ok {
		t.Error("key should not exist after delete")
	}
	if val != nil {
		t.Error("value should be nil")
	}
}

func TestSkipList_DeleteNonExistent(t *testing.T) {
	sl := NewSkipList()
	sl.Delete("nonexistent")

	entries := sl.Iter()
	if len(entries) != 1 {
		t.Errorf("expected 1 tombstone entry, got %d", len(entries))
	}
	if !entries[0].Deleted {
		t.Error("entry should be marked as deleted")
	}
}

func TestSkipList_DeleteMarkedDeleted(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))
	sl.Delete("key1")
	sl.Delete("key1")

	val, ok := sl.Get("key1")
	if ok {
		t.Error("key should not exist")
	}
	if val != nil {
		t.Error("value should be nil")
	}
}

func TestSkipList_Iter(t *testing.T) {
	sl := NewSkipList()
	entries := sl.Iter()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestSkipList_IterSorted(t *testing.T) {
	sl := NewSkipList()
	sl.Set("z", []byte("1"))
	sl.Set("a", []byte("2"))
	sl.Set("m", []byte("3"))

	entries := sl.Iter()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if entries[0].Key != "a" || entries[1].Key != "m" || entries[2].Key != "z" {
		t.Errorf("entries not in sorted order: %v", entries)
	}
}

func TestSkipList_IterIncludesTombstones(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key1", []byte("value1"))
	sl.Delete("key1")

	entries := sl.Iter()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry with tombstone, got %d", len(entries))
	}
	if !entries[0].Deleted {
		t.Error("entry should be marked as deleted")
	}
}

func TestSkipList_Size(t *testing.T) {
	sl := NewSkipList()
	if sl.Size() != 0 {
		t.Errorf("expected size 0, got %d", sl.Size())
	}

	sl.Set("key", []byte("value"))
	if sl.Size() <= 0 {
		t.Error("size should be positive after set")
	}
}

func TestSkipList_SizeAfterDelete(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte("value"))
	initialSize := sl.Size()

	sl.Delete("key")

	if sl.Size() >= initialSize {
		t.Error("size should decrease after delete")
	}
}

func TestSkipList_SetDuplicateKey(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte("value1"))
	sl.Set("key", []byte("value2"))

	val, ok := sl.Get("key")
	if !ok {
		t.Error("key should exist")
	}
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got %s", string(val))
	}
}

func TestSkipList_SetAfterDelete(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte("value1"))
	sl.Delete("key")
	sl.Set("key", []byte("value2"))

	val, ok := sl.Get("key")
	if !ok {
		t.Error("key should exist")
	}
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got %s", string(val))
	}
}

func TestSkipList_EmptyValue(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte(""))

	val, ok := sl.Get("key")
	if !ok {
		t.Error("key should exist")
	}
	if len(val) != 0 {
		t.Errorf("expected empty value, got %d bytes", len(val))
	}
}

func TestSkipList_LargeKeys(t *testing.T) {
	sl := NewSkipList()
	largeKey := string(make([]byte, 10000))
	largeValue := string(make([]byte, 10000))

	sl.Set(largeKey, []byte(largeValue))

	val, ok := sl.Get(largeKey)
	if !ok {
		t.Error("key should exist")
	}
	if string(val) != largeValue {
		t.Error("value should match")
	}
}

func TestSkipList_ManyEntries(t *testing.T) {
	sl := NewSkipList()
	count := 1000

	for i := 0; i < count; i++ {
		key := string(rune('a' + i%26))
		sl.Set(key, []byte(strconv.Itoa(i)))
	}

	for i := 0; i < count; i++ {
		key := string(rune('a' + i%26))
		val, ok := sl.Get(key)
		if !ok {
			t.Errorf("key %s should exist", key)
		}
		if len(val) == 0 {
			t.Errorf("key %s should have value", key)
		}
	}

	entries := sl.Iter()
	if len(entries) != 26 {
		t.Errorf("expected 26 entries, got %d", len(entries))
	}
}

func TestSkipList_Concurrent(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 10; i++ {
		go func(idx int) {
			sl.Set(string(rune('a'+idx)), []byte(string(rune('0'+idx))))
		}(i)
	}

	_, _ = sl.Get("a")
}

func TestSkipList_GetDeletedKey(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte("value"))
	sl.Delete("key")

	val, ok := sl.Get("key")
	if ok {
		t.Error("deleted key should not be found")
	}
	if val != nil {
		t.Error("value should be nil for deleted key")
	}
}

func TestSkipList_SetDeletedKey(t *testing.T) {
	sl := NewSkipList()
	sl.Set("key", []byte("value1"))
	sl.Delete("key")
	sl.Set("key", []byte("value2"))

	val, ok := sl.Get("key")
	if !ok {
		t.Error("key should exist after re-insert")
	}
	if string(val) != "value2" {
		t.Errorf("expected 'value2', got %s", string(val))
	}
}

func TestSkipList_IterOrder(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"c", "a", "d", "b"}

	for _, k := range keys {
		sl.Set(k, []byte(k+"_value"))
	}

	entries := sl.Iter()
	if len(entries) != len(keys) {
		t.Fatalf("expected %d entries, got %d", len(keys), len(entries))
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Key < entries[i-1].Key {
			t.Errorf("entries not in sorted order at index %d: %s < %s",
				i-1, entries[i].Key, entries[i-1].Key)
		}
	}
}

func TestSkipList_SizeAccurate(t *testing.T) {
	sl := NewSkipList()
	initialSize := sl.Size()

	sl.Set("a", []byte("1"))
	sl.Set("b", []byte("22"))
	sl.Set("c", []byte("333"))

	sizeAfterSets := sl.Size()
	expectedSize := initialSize + int64(len("a")+1) + int64(len("b")+2) + int64(len("c")+3)
	if sizeAfterSets != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, sizeAfterSets)
	}

	sl.Delete("a")
	sizeAfterDelete := sl.Size()
	if sizeAfterDelete >= sizeAfterSets {
		t.Error("size should decrease after delete")
	}
}
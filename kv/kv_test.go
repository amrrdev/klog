package kv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestKV_SetAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("expected key1 to exist")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %q", v)
	}
}

func TestKV_GetNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	v, ok := kv.Get("nonexistent")
	if ok {
		t.Error("expected key to not exist")
	}
	if v != "" {
		t.Errorf("expected empty value, got %q", v)
	}
}

func TestKV_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %q", v)
	}

	err = kv.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	v, ok = kv.Get("key1")
	if ok {
		t.Error("key1 should not exist after delete")
	}
}

func TestKV_DeleteNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Delete("nonexistent")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestKV_EmptyValue(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "" {
		t.Errorf("expected empty value, got %q", v)
	}
}

func TestKV_EmptyKey(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("", "value")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	v, ok := kv.Get("")
	if !ok {
		t.Error("empty key should exist")
	}
	if v != "value" {
		t.Errorf("expected 'value', got %q", v)
	}
}

func TestKV_MultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	keys := []string{"a", "b", "c", "d", "e"}
	values := []string{"1", "2", "3", "4", "5"}

	for i := range keys {
		err := kv.Set(keys[i], values[i])
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	for i := range keys {
		v, ok := kv.Get(keys[i])
		if !ok {
			t.Errorf("key %s should exist", keys[i])
		}
		if v != values[i] {
			t.Errorf("key %s: expected %s, got %s", keys[i], values[i], v)
		}
	}
}

func TestKV_RecoveryAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	kv1, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}

	err = kv1.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	err = kv1.Set("key2", "value2")
	if err != nil {
		t.Fatalf("Set2 failed: %v", err)
	}
	err = kv1.Delete("key2")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	err = kv1.Set("key3", "value3")
	if err != nil {
		t.Fatalf("Set3 failed: %v", err)
	}

	kv1.Close()

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	v, ok := kv2.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %q", v)
	}

	v, ok = kv2.Get("key2")
	if ok {
		t.Error("key2 should not exist (deleted)")
	}

	v, ok = kv2.Get("key3")
	if !ok {
		t.Error("key3 should exist")
	}
	if v != "value3" {
		t.Errorf("expected 'value3', got %q", v)
	}
}

func TestKV_RecoveryAfterMultipleReopens(t *testing.T) {
	tmpDir := t.TempDir()

	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	kv.Close()

	kv, err = OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	err = kv.Set("key2", "value2")
	if err != nil {
		t.Fatalf("Set2 failed: %v", err)
	}
	kv.Close()

	kv, err = OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore3 failed: %v", err)
	}
	err = kv.Set("key3", "value3")
	if err != nil {
		t.Fatalf("Set3 failed: %v", err)
	}
	kv.Close()

	kv, err = OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore4 failed: %v", err)
	}
	defer kv.Close()

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %q", v)
	}

	v, ok = kv.Get("key2")
	if !ok {
		t.Error("key2 should exist")
	}
	if v != "value2" {
		t.Errorf("expected 'value2', got %q", v)
	}

	v, ok = kv.Get("key3")
	if !ok {
		t.Error("key3 should exist")
	}
	if v != "value3" {
		t.Errorf("expected 'value3', got %q", v)
	}
}

func TestKV_OverwriteValue(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %q", v)
	}

	err = kv.Set("key1", "value2")
	if err != nil {
		t.Fatalf("Set2 failed: %v", err)
	}

	v, ok = kv.Get("key1")
	if !ok {
		t.Error("key1 should exist after overwrite")
	}
	if v != "value2" {
		t.Errorf("expected 'value2', got %q", v)
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	v, ok = kv2.Get("key1")
	if !ok {
		t.Error("key1 should exist after recovery")
	}
	if v != "value2" {
		t.Errorf("expected 'value2', got %q", v)
	}
}

func TestKV_RecoveryWithCorruptedTail(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	_ = kv.Close()
}

func TestKV_DeleteAfterSet(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kv.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if ok {
		t.Error("key1 should not exist after delete")
	}
	if v != "" {
		t.Errorf("expected empty value, got %q", v)
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	v, ok = kv2.Get("key1")
	if ok {
		t.Error("key1 should not exist after recovery")
	}
}

func TestKV_AllTypesOfValues(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	tests := []struct {
		key   string
		value string
	}{
		{"empty", ""},
		{"space", " "},
		{"special", "!@#$%^&*()"},
		{"unicode", "hello world ñ"},
		{"newline", "line1\nline2"},
		{"tab", "col1\tcol2"},
	}

	for _, tc := range tests {
		err := kv.Set(tc.key, tc.value)
		if err != nil {
			t.Fatalf("Set failed for %q: %v", tc.key, err)
		}
	}

	for _, tc := range tests {
		v, ok := kv.Get(tc.key)
		if !ok {
			t.Errorf("key %q should exist", tc.key)
		}
		if v != tc.value {
			t.Errorf("key %q: expected %q, got %q", tc.key, tc.value, v)
		}
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	for _, tc := range tests {
		v, ok := kv2.Get(tc.key)
		if !ok {
			t.Errorf("key %q should exist after recovery", tc.key)
		}
		if v != tc.value {
			t.Errorf("key %q after recovery: expected %q, got %q", tc.key, tc.value, v)
		}
	}
}

func TestKV_DeleteThenSet(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	err = kv.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	err = kv.Set("key1", "value2")
	if err != nil {
		t.Fatalf("Set2 failed: %v", err)
	}

	v, ok := kv.Get("key1")
	if !ok {
		t.Error("key1 should exist")
	}
	if v != "value2" {
		t.Errorf("expected 'value2', got %q", v)
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	v, ok = kv2.Get("key1")
	if !ok {
		t.Error("key1 should exist after recovery")
	}
	if v != "value2" {
		t.Errorf("expected 'value2', got %q", v)
	}
}

func TestKV_MixedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	err = kv.Set("a", "1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	err = kv.Set("b", "2")
	if err != nil {
		t.Fatalf("Set2 failed: %v", err)
	}
	err = kv.Delete("a")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	err = kv.Set("c", "3")
	if err != nil {
		t.Fatalf("Set3 failed: %v", err)
	}
	err = kv.Delete("b")
	if err != nil {
		t.Fatalf("Delete2 failed: %v", err)
	}
	err = kv.Set("d", "4")
	if err != nil {
		t.Fatalf("Set4 failed: %v", err)
	}

	_, ok := kv.Get("a")
	if ok {
		t.Error("key 'a' should not exist")
	}

	_, ok = kv.Get("b")
	if ok {
		t.Error("key 'b' should not exist")
	}

	v, ok := kv.Get("c")
	if !ok {
		t.Error("key 'c' should exist")
	}
	if v != "3" {
		t.Errorf("expected '3', got %q", v)
	}

	v, ok = kv.Get("d")
	if !ok {
		t.Error("key 'd' should exist")
	}
	if v != "4" {
		t.Errorf("expected '4', got %q", v)
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	_, ok = kv2.Get("a")
	if ok {
		t.Error("key 'a' should not exist after recovery")
	}

	_, ok = kv2.Get("b")
	if ok {
		t.Error("key 'b' should not exist after recovery")
	}

	v, ok = kv2.Get("c")
	if !ok {
		t.Error("key 'c' should exist after recovery")
	}
	if v != "3" {
		t.Errorf("expected '3', got %q", v)
	}

	v, ok = kv2.Get("d")
	if !ok {
		t.Error("key 'd' should exist after recovery")
	}
	if v != "4" {
		t.Errorf("expected '4', got %q", v)
	}
}

func TestKV_CloseWithoutDataLoss(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}

	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	values := []string{"v1", "v2", "v3", "v4", "v5"}

	for i := range keys {
		err := kv.Set(keys[i], values[i])
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	kv.Close()

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	for i := range keys {
		v, ok := kv2.Get(keys[i])
		if !ok {
			t.Errorf("key %s should exist after close and reopen", keys[i])
		}
		if v != values[i] {
			t.Errorf("key %s: expected %s, got %s", keys[i], values[i], v)
		}
	}
}

func TestKV_EmptyStore(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	v, ok := kv.Get("any")
	if ok {
		t.Error("expected no keys")
	}
	if v != "" {
		t.Errorf("expected empty value, got %q", v)
	}
}

func TestKV_ReopenEmptyStore(t *testing.T) {
	tmpDir := t.TempDir()
	kv1, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	kv1.Close()

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	v, ok := kv2.Get("any")
	if ok {
		t.Error("expected no keys")
	}
	if v != "" {
		t.Errorf("expected empty value, got %q", v)
	}
}

func TestKV_ManyKeys(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}
	defer kv.Close()

	for i := 0; i < 100; i++ {
		key := string(rune('a' + i))
		value := string(rune('0' + i%10))
		err := kv.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	for i := 0; i < 100; i++ {
		key := string(rune('a' + i))
		expected := string(rune('0' + i%10))
		v, ok := kv2.Get(key)
		if !ok {
			t.Errorf("key %s should exist", key)
		}
		if v != expected {
			t.Errorf("key %s: expected %s, got %s", key, expected, v)
		}
	}
}

func TestKV_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	kv1, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}

	for i := 0; i < 26; i++ {
		err := kv1.Set(string(rune('a'+i)), string(rune('0'+i%10)))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	kv1.Close()

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	for i := 0; i < 26; i++ {
		key := string(rune('a' + i))
		expected := string(rune('0' + i%10))
		v, ok := kv2.Get(key)
		if !ok {
			t.Errorf("key %s should exist", key)
		}
		if v != expected {
			t.Errorf("key %s: expected %s, got %s", key, expected, v)
		}
	}
}

func TestKV_PanicOnUnrecoverableError(t *testing.T) {
	tmpDir := t.TempDir()
	kv, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore failed: %v", err)
	}

	err = kv.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	kv.Close()

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected log file")
	}

	os.Rename(files[0], files[0]+".backup")

	kv2, err := OpenKVStore(tmpDir)
	if err != nil {
		t.Fatalf("OpenKVStore2 failed: %v", err)
	}
	defer kv2.Close()

	_, ok := kv2.Get("key1")
	if ok {
		t.Error("key should not exist after missing segment")
	}
}

package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_OpenNew(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	_, err = w.Write(TypeWrite, []byte("test payload"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWAL_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	var records []Record
	err = w.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}

	_, err = w.Write(TypeWrite, []byte(`{"k":"key1","v":"value1"}`))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err = w.Write(TypeDelete, []byte(`{"k":"key2"}`))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err = w.Write(TypeWrite, []byte(`{"k":"key3","v":"value3"}`))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	w.Close()

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	records = nil
	err = w2.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover2 failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if records[0].Type != TypeWrite {
		t.Errorf("expected TypeWrite, got %d", records[0].Type)
	}
	if string(records[0].Payload) != `{"k":"key1","v":"value1"}` {
		t.Errorf("expected payload, got %s", string(records[0].Payload))
	}

	if records[1].Type != TypeDelete {
		t.Errorf("expected TypeDelete, got %d", records[1].Type)
	}

	if records[2].Type != TypeWrite {
		t.Errorf("expected TypeWrite, got %d", records[2].Type)
	}
}

func TestWAL_EmptyPayload(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	_, err = w.Write(TypeWrite, []byte(""))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
}

func TestWAL_LargePayload(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	largePayload := make([]byte, 1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	_, err = w.Write(TypeWrite, largePayload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	var recovered Record
	err = w2.Recover(func(rec Record) error {
		recovered = rec
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(recovered.Payload) != len(largePayload) {
		t.Errorf("payload length mismatch: got %d, want %d", len(recovered.Payload), len(largePayload))
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	payload := make([]byte, 64*1024*1024)
	_, err = w.Write(TypeWrite, payload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, err = w.Write(TypeWrite, []byte("trigger rotation"))
	if err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) <= 1 {
		t.Errorf("expected multiple segments after rotation, got %d", len(files))
	}
}

func TestWAL_TruncateCorruptedTail(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, err = w.Write(TypeWrite, []byte("valid record"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_ = w.Close()
}

func TestWAL_MultipleSegmentsRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		payload := []byte(string(rune('a' + i%26)))
		_, err := w.Write(TypeWrite, payload)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	w.Close()

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	var records []Record
	err = w2.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 20 {
		t.Errorf("expected 20 records, got %d", len(records))
	}
}

func TestWAL_DifferentRecordTypes(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	tests := []struct {
		recType uint8
		payload []byte
	}{
		{TypeWrite, []byte("write data")},
		{TypeDelete, []byte("delete data")},
		{TypeCommit, []byte("commit data")},
		{TypeWrite, []byte("another write")},
		{TypeDelete, []byte("another delete")},
	}

	for _, tc := range tests {
		_, err := w.Write(tc.recType, tc.payload)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	var records []Record
	err = w2.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != len(tests) {
		t.Fatalf("expected %d records, got %d", len(tests), len(records))
	}

	for i, tc := range tests {
		if records[i].Type != tc.recType {
			t.Errorf("record %d: expected type %d, got %d", i, tc.recType, records[i].Type)
		}
		if string(records[i].Payload) != string(tc.payload) {
			t.Errorf("record %d: expected payload %s, got %s", i, tc.payload, records[i].Payload)
		}
	}
}

func TestWAL_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	var records []Record
	err = w.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}

func TestWAL_RecoveryReturnsLSN(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	payloads := []string{"first", "second", "third"}
	for _, p := range payloads {
		_, err := w.Write(TypeWrite, []byte(p))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	var records []Record
	err = w.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	expectedLSN := uint64(0)
	for i := range records {
		if records[i].LSN != expectedLSN {
			t.Errorf("record %d: expected LSN %d, got %d", i, expectedLSN, records[i].LSN)
		}
		expectedLSN += uint64(headerSize + len(records[i].Payload))
	}
}

func TestWAL_CloseAndReopen(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	_, err = w.Write(TypeWrite, []byte("before close"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	w.Close()

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}

	_, err = w2.Write(TypeWrite, []byte("after reopen"))
	if err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}

	w2.Close()

	w3, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open3 failed: %v", err)
	}
	defer w3.Close()

	var records []Record
	err = w3.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if string(records[0].Payload) != "before close" {
		t.Errorf("expected 'before close', got %s", string(records[0].Payload))
	}
	if string(records[1].Payload) != "after reopen" {
		t.Errorf("expected 'after reopen', got %s", string(records[1].Payload))
	}
}

func TestWAL_MissingSegmentFiles(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	_, err = w.Write(TypeWrite, []byte("test"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected at least one file")
	}

	segmentName := files[0]
	os.Remove(segmentName)

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	_, err = w2.Write(TypeWrite, []byte("new"))
	if err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}

	files2, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Glob2 failed: %v", err)
	}
	if len(files2) == 0 {
		t.Error("expected at least one file after write")
	}
}

func TestDecode_TruncatedHeader(t *testing.T) {
	buf := make([]byte, 5)
	_, err := decode(bytes.NewReader(buf))
	if err == nil {
		t.Error("expected error for truncated header")
	}
}

func TestDecode_TruncatedPayload(t *testing.T) {
	header := make([]byte, headerSize)
	header[8] = TypeWrite
	binary.LittleEndian.PutUint32(header[13:17], uint32(100))
	reader := bytes.NewReader(header)
	_, err := decode(reader)
	if err == nil {
		t.Error("expected error for truncated payload")
	}
}

func TestEncode_Deterministic(t *testing.T) {
	rec1 := Record{LSN: 100, Type: TypeWrite, Payload: []byte("test")}
	rec2 := Record{LSN: 100, Type: TypeWrite, Payload: []byte("test")}

	encoded1 := encode(rec1)
	encoded2 := encode(rec2)

	if string(encoded1) != string(encoded2) {
		t.Error("encode not deterministic")
	}
}

func TestWAL_AllZeroLSN(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	lsn, err := w.Write(TypeWrite, []byte("first"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected first LSN 0, got %d", lsn)
	}

	lsn, err = w.Write(TypeWrite, []byte("second"))
	if err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}
	if lsn != uint64(headerSize+5) {
		t.Errorf("expected second LSN %d, got %d", headerSize+5, lsn)
	}
}

func TestWAL_WriteReturnsLSN(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	lsn1, err := w.Write(TypeWrite, []byte("test1"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	lsn2, err := w.Write(TypeWrite, []byte("test2"))
	if err != nil {
		t.Fatalf("Write2 failed: %v", err)
	}

	if lsn2 <= lsn1 {
		t.Errorf("expected lsn2 > lsn1, got %d <= %d", lsn2, lsn1)
	}
}

func TestWAL_RecoveryWithMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		payload := make([]byte, 16*1024*1024)
		_, err := w.Write(TypeWrite, payload)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	w.Close()

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	count := 0
	err = w2.Recover(func(rec Record) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 records, got %d", count)
	}
}

func TestWAL_DeleteThenWrite(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	_, err = w.Write(TypeDelete, []byte("key1"))
	if err != nil {
		t.Fatalf("Write delete failed: %v", err)
	}

	_, err = w.Write(TypeWrite, []byte("key1=val1"))
	if err != nil {
		t.Fatalf("Write write failed: %v", err)
	}

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	var records []Record
	err = w2.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Type != TypeDelete {
		t.Errorf("first record should be delete, got %d", records[0].Type)
	}
	if records[1].Type != TypeWrite {
		t.Errorf("second record should be write, got %d", records[1].Type)
	}
}

func TestWAL_SequentialWrites(t *testing.T) {
	tmpDir := t.TempDir()
	w, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		_, err := w.Write(TypeWrite, []byte(string(rune('a'+i))))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	w2, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Open2 failed: %v", err)
	}
	defer w2.Close()

	var records []Record
	err = w2.Recover(func(rec Record) error {
		records = append(records, rec)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if len(records) != 5 {
		t.Errorf("expected 5 records, got %d", len(records))
	}
}

package wal

import (
	"bytes"
	"testing"
)

func TestEncode(t *testing.T) {
	rec := Record{
		LSN:     100,
		Type:    TypeWrite,
		Payload: []byte("test payload"),
	}

	encoded := encode(rec)

	if len(encoded) != headerSize+len(rec.Payload) {
		t.Errorf("encoded length mismatch: got %d, want %d", len(encoded), headerSize+len(rec.Payload))
	}
}

func TestDecode(t *testing.T) {
	original := Record{
		LSN:     100,
		Type:    TypeWrite,
		Payload: []byte("test payload"),
	}

	encoded := encode(original)

	decoded, err := decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.LSN != original.LSN {
		t.Errorf("LSN mismatch: got %d, want %d", decoded.LSN, original.LSN)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type mismatch: got %d, want %d", decoded.Type, original.Type)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Errorf("Payload mismatch: got %s, want %s", string(decoded.Payload), string(original.Payload))
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []Record{
		{LSN: 0, Type: TypeWrite, Payload: []byte("")},
		{LSN: 1, Type: TypeDelete, Payload: []byte("key")},
		{LSN: 100, Type: TypeWrite, Payload: []byte("some data")},
		{LSN: 1000, Type: TypeCommit, Payload: []byte("commit data")},
		{LSN: 10000, Type: TypeWrite, Payload: make([]byte, 1000)},
	}

	for _, original := range tests {
		encoded := encode(original)
		decoded, err := decode(bytes.NewReader(encoded))
		if err != nil {
			t.Errorf("decode failed for LSN %d: %v", original.LSN, err)
			continue
		}

		if decoded.LSN != original.LSN {
			t.Errorf("LSN mismatch for original LSN %d: got %d", original.LSN, decoded.LSN)
		}
		if decoded.Type != original.Type {
			t.Errorf("Type mismatch: got %d, want %d", decoded.Type, original.Type)
		}
		if !bytes.Equal(decoded.Payload, original.Payload) {
			t.Errorf("Payload mismatch for LSN %d", original.LSN)
		}
	}
}

func TestChecksumMismatch(t *testing.T) {
	rec := Record{
		LSN:     100,
		Type:    TypeWrite,
		Payload: []byte("test payload"),
	}

	encoded := encode(rec)

	encoded[15] = ^encoded[15]

	_, err := decode(bytes.NewReader(encoded))
	if err == nil {
		t.Error("expected error for checksum mismatch")
	}
}

func TestHeaderSize(t *testing.T) {
	if headerSize != 17 {
		t.Errorf("expected headerSize 17, got %d", headerSize)
	}
}

func TestRecordTypes(t *testing.T) {
	if TypeWrite != 1 {
		t.Errorf("expected TypeWrite 1, got %d", TypeWrite)
	}
	if TypeDelete != 2 {
		t.Errorf("expected TypeDelete 2, got %d", TypeDelete)
	}
	if TypeCommit != 3 {
		t.Errorf("expected TypeCommit 3, got %d", TypeCommit)
	}
}

func TestDecodeEmptyPayload(t *testing.T) {
	rec := Record{
		LSN:     100,
		Type:    TypeWrite,
		Payload: []byte{},
	}

	encoded := encode(rec)

	decoded, err := decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(decoded.Payload))
	}
}

func TestDecodeLargePayload(t *testing.T) {
	largePayload := make([]byte, 1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	rec := Record{
		LSN:     0,
		Type:    TypeWrite,
		Payload: largePayload,
	}

	encoded := encode(rec)

	decoded, err := decode(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !bytes.Equal(decoded.Payload, largePayload) {
		t.Error("payload mismatch for large payload")
	}
}

func TestEncodeLittleEndian(t *testing.T) {
	rec := Record{
		LSN:     0x123456789ABCDEF0,
		Type:    TypeWrite,
		Payload: []byte("test"),
	}

	encoded := encode(rec)

	if encoded[0] != 0xF0 {
		t.Errorf("LSN byte 0: expected 0xF0, got 0x%02X", encoded[0])
	}
	if encoded[1] != 0xDE {
		t.Errorf("LSN byte 1: expected 0xDE, got 0x%02X", encoded[1])
	}
}

func TestDecodeCorruptedLength(t *testing.T) {
	rec := Record{
		LSN:     100,
		Type:    TypeWrite,
		Payload: []byte("test"),
	}

	encoded := encode(rec)

	encoded[13] = 0xFF
	encoded[14] = 0xFF
	encoded[15] = 0xFF
	encoded[16] = 0xFF

	_, err := decode(bytes.NewReader(encoded))
	if err == nil {
		t.Error("expected error for corrupted length")
	}
}

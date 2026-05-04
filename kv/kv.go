package kv

import (
	"encoding/json"
	"fmt"

	"github.com/amrrdev/wal/wal"
)

type KVPayload struct {
	Key   string `json:"k"`
	Value string `json:"v,omitempty"`
}

type KVStore struct {
	data map[string]string
	wal  *wal.WAL
}

// KVStore is an in-memory key-value store backed by a WAL.
// On startup it replays the WAL to reconstruct its state.
// Every write is immediately durable.
func OpenKVStore(dir string) (*KVStore, error) {
	w, err := wal.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	kv := &KVStore{
		data: make(map[string]string),
		wal:  w,
	}

	// Replay the WAL to reconstruct in-memory state
	if err := w.Recover(kv.apply); err != nil {
		return nil, fmt.Errorf("recover: %w", err)
	}

	return kv, nil
}

func (kv *KVStore) apply(rec wal.Record) error {
	var p KVPayload
	if err := json.Unmarshal(rec.Payload, &p); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	switch rec.Type {
	case wal.TypeWrite:
		kv.data[p.Key] = p.Value
	case wal.TypeDelete:
		delete(kv.data, p.Key)
	}

	return nil
}

func (kv *KVStore) Set(key, value string) error {
	payload, err := json.Marshal(KVPayload{Key: key, Value: value})
	if err != nil {
		return err
	}

	if _, err := kv.wal.Write(wal.TypeWrite, payload); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	// Only update the in-memory map after the WAL write succeeds.
	// If the WAL write fails (disk full, fsync error), we do not update
	// the map — the store stays consistent with what is on disk.
	kv.data[key] = value
	return nil
}

func (kv *KVStore) Delete(key string) error {
	payload, err := json.Marshal(KVPayload{Key: key})
	if err != nil {
		return err
	}

	if _, err := kv.wal.Write(wal.TypeDelete, payload); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	delete(kv.data, key)
	return nil
}

func (kv *KVStore) Get(key string) (string, bool) {
	v, ok := kv.data[key]
	return v, ok
}

func (kv *KVStore) Close() error {
	return kv.wal.Close()
}

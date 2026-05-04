# klog

klog is a persistent key-value store that guarantees durability by writing every mutation to a Write-Ahead Log (WAL) with fsync before acknowledging the write. On crash, klog automatically recovers by replaying the WAL and truncating partial records.

Built from scratch to learn database internals, klog includes:

- WAL with segment rotation (64MB files)
- CRC32-Castagnoli integrity checking
- Crash-safe recovery with tail truncation
- In-memory store rebuilt from WAL on startup

## Record Format

```
┌──────────┬────────┬──────────┬────────────┬──────────────────┐
│  LSN     │  Type  │   CRC32  │   Length   │     Payload      │
│ uint64   │ uint8  │  uint32  │   uint32   │   [Length]byte   │
│ 8 bytes  │ 1 byte │ 4 bytes  │  4 bytes   │     N bytes      │
└──────────┴────────┴──────────┴────────────┴──────────────────┘
```

| Field     | Size     | Description                                      |
|-----------|----------|--------------------------------------------------|
| LSN       | 8 bytes  | Byte offset of the record in the log            |
| Type      | 1 byte   | 1=Write, 2=Delete, 3=Commit                     |
| CRC32     | 4 bytes  | Castagnoli polynomial checksum of payload       |
| Length    | 4 bytes  | Size of the payload in bytes                    |
| Payload   | N bytes  | The actual data (JSON-encoded KV pair)          |

All multi-byte fields are encoded in little-endian byte order.

## Next

SSTables, memtable (skip list), and full LSM-tree implementation.

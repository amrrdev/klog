# KVL - Persistent Key-Value Store

A production-grade persistent key-value store built from scratch to learn database internals. KVL guarantees durability through Write-Ahead Logging (WAL), implements an in-memory Memtable with SkipList, and persists data to SSTables - forming a complete LSM-tree architecture.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         KVL Architecture                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐        │
│  │    Set()    │────▶│   Memtable  │────▶│     WAL     │        │
│  │   Delete()  │     │ (SkipList)  │     │  (Durable)  │        │
│  │    Get()    │◀────│  (In-Memory)│     │   (Sync)    │        │
│  └─────────────┘     └─────────────┘     └─────────────┘        │
│         │                   │                     │             │
│         │                   ▼                     │             │
│         │            ┌─────────────┐              │             │
│         │            │   SSTable   │◀─────────────┘             │
│         │            │  (On Disk)  │                            │
│         │            └─────────────┘                            │
│         │                   │                                   │
│         ▼                   ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                     Read Path                           │    │
│  │  1. Check Memtable (SkipList)                           │    │
│  │  2. Check SSTables (Newest → Oldest)                    │    │
│  │  3. Bloom Filter for fast membership                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Features Implemented

### Write-Ahead Log (WAL)
- **Durability**: Every write is fsync'd to disk before acknowledgment
- **Segment Rotation**: Automatic 64MB segment files
- **Crash Recovery**: Replays WAL on startup, truncates partial records
- **Integrity**: CRC32-Castagnoli checksum for corruption detection

### Memtable (SkipList)
- **In-Memory Storage**: Fast write performance
- **Sorted Keys**: O(log n) search, insert, and delete
- **Tombstones**: Logical deletion support for SSTable persistence
- **Size Tracking**: Flush threshold monitoring

### SSTable (Sorted String Table)
- **Immutable**: Once written, never modified
- **Bloom Filter**: Fast key existence checks (1% false positive)
- **Index Block**: Fast key lookup within file
- **Data Block**: Sorted key-value pairs

---

## Data Formats

### WAL Record Format

```
┌──────────┬────────┬──────────┬────────────┬──────────────────┐
│  LSN     │  Type  │   CRC32  │   Length   │     Payload      │
│ uint64   │ uint8  │  uint32  │   uint32   │   [Length]byte   │
│ 8 bytes  │ 1 byte │ 4 bytes  │  4 bytes   │     N bytes      │
└──────────┴────────┴──────────┴────────────┴──────────────────┘
```

| Field     | Size     | Description                                      |
|-----------|----------|--------------------------------------------------|
| LSN       | 8 bytes  | Byte offset of the record in the log             |
| Type      | 1 byte   | 1=Write, 2=Delete, 3=Commit                      |
| CRC32     | 4 bytes  | Castagnoli polynomial checksum of payload        |
| Length    | 4 bytes  | Size of the payload in bytes                     |
| Payload   | N bytes  | The actual data (JSON-encoded KV pair)          |

All multi-byte fields are encoded in little-endian byte order.

---

### SSTable File Format

```
┌───────────────────────────────────────────────────────────────────┐
│                         SSTable File                              │
├─────────────────────┬───────────────────┬─────────────────────────┤
│   Data Block        │   Index Block     │    Footer (24 bytes)    │
│  (Key-Value Pairs)  │  (Key → Offset)   │                         │
│                     │                   │  ┌─────────┬────────┐   │
│ ┌────────┬────────┐ │ ┌──────┬───────┐  │  │ Index   │ Bloom  │   │
│ │ KeyLen │ Value  │ │ │KeyLen│Offset │  │  │ Offset  │ Offset │   │
│ │  uint32│ [N]byte│ │ │uint32│ uint64│  │  │ uint64  │ uint64 │   │
│ └────────┴────────┘ │ └──────┴───────┘  │  └─────────┴────────┘   │
│        ...          │        ...        │  ┌──────────────────┐   │
│                     │                   │  │     Magic        │   │
│                     │                   │  │ 0xDEADC0FFEE     │   │
│                     │                   │  └──────────────────┘   │
└─────────────────────┴───────────────────┴─────────────────────────┘
```

#### Data Block Entry

```
┌──────────┬───────────┬─────────┬─────────────┬──────────────────┐
│ KeyLen   │ ValueLen  │ Deleted │    Key      │      Value       │
│ uint32   │  uint32   │ uint8   │ [KeyLen]byte│  [ValueLen]byte  │
│ 4 bytes  │  4 bytes  │ 1 byte  │   N bytes   │    M bytes       │
└──────────┴───────────┴─────────┴─────────────┴──────────────────┘
```

#### Index Block Entry

```
┌──────────┬───────────┬──────────────────┐
│ KeyLen   │  Offset   │       Key        │
│ uint32   │  uint64   │  [KeyLen]byte    │
│ 4 bytes  │  8 bytes  │    N bytes       │
└──────────┴───────────┴──────────────────┘
```

#### Footer (24 bytes)

| Field     | Size     | Description                         |
|-----------|----------|-------------------------------------|
| Index Offset | 8 bytes  | Byte offset where index block starts |
| Bloom Offset| 8 bytes  | Byte offset where bloom filter starts |
| Magic    | 8 bytes  | 0xDEADC0FFEE (file validation)      |

---

### SkipList Node Structure

```
┌──────────┬───────────┬─────────┬────────────────────────────┐
│   Key    │   Value   │ Deleted │        Forward[]           │
│  string  │  []byte   │  bool   │   []*node (level pointers) │
└──────────┴───────────┴─────────┴────────────────────────────┘

Level 3: head ──────────────────────────────▶ node3 ──▶ nil
Level 2: head ────────▶ node1 ──▶ node2 ──▶ node3 ──▶ nil
Level 1: head ──▶ node1 ──▶ node2 ──▶ node3 ──▶ node4 ──▶ nil
```

- **maxLevel**: 16 (handles ~65,000 entries efficiently)
- **probability**: 0.25 for cache-friendly wider structure

---

### Bloom Filter Format

```
┌──────────────────┬────────────────────┬────────────────────────┐
│       k          │         m          │          bits          │
│     uint64       │       uint64       │      [m/8]bytes        │
│  (num hashes)    │   (num bits)       │   (bit array)          │
└──────────────────┴────────────────────┴────────────────────────┘
```

- **k**: Number of hash functions (≈7 for 1% FPR)
- **m**: Number of bits (≈10n for n keys at 1% FPR)
- **bits**: Bit array for membership testing

---

## Component Details

### WAL (`wal/`)
- `wal.go`: Segment management, write coordination, recovery
- `record.go`: Binary encoding/decoding with CRC32 integrity

### Key-Value Store (`kv/`)
- `kv.go`: In-memory KV store backed by WAL

### Memtable (`memtable/`)
- `memtable.go`: Flush threshold management, entry iteration
- `skiplist.go`: Ordered key storage with O(log n) operations

### SSTable (`sstable/`)
- `writer.go`: Multi-phase flush (data → index → bloom → footer)
- `reader.go`: Key lookup with bloom filter and index search
- `flush.go`: Memtable to SSTable conversion
- `bloom.go`: Space-efficient membership testing

---

## Recovery Process

1. **WAL Recovery**: Replay all segments in LSN order, truncate corrupt tail
2. **Memtable Rebuild**: Apply WAL records to in-memory SkipList
3. **SSTable Discovery**: Load all .sst files in directory

## Read Path

1. Check Memtable (SkipList) - newest data in memory
2. Check each SSTable newest to oldest
3. Use Bloom filter to skip irrelevant SSTables
4. Use index block to find data block position

## Build & Test

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test -v ./wal/...
```

---

## License

MIT License - Built for learning database internals.

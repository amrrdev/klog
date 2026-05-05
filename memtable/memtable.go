package memtable

const (
	// defaultMaxSize is the memtable flush threshold in bytes.
	// 4MB matches LevelDB's default write_buffer_size.
	// RocksDB defaults to 64MB. Larger memtables mean fewer SSTables
	// (better read performance) but higher memory usage and longer
	// recovery time if the process crashes.
	defaultMaxSize = 64 * 1024 * 1024
)

// Memtable is the in-memory write buffer for the storage engine.
// Writes go here after the WAL. Reads check here before checking SSTables.
// When Size() exceeds MaxSize, the engine flushes this memtable to an SSTable
// and replaces it with a fresh one.
type Memtable struct {
	sl      *SkipList
	MaxSize int64
}

func New() *Memtable {
	return &Memtable{
		sl:      NewSkipList(),
		MaxSize: defaultMaxSize,
	}
}

// Iter returns all entries in sorted key order, including tombstones.
// Called by the flush path to write the memtable contents to an SSTable.
func (m *Memtable) Iter() []Entry {
	return m.sl.Iter()
}

// Size returns the approximate byte size of the memtable contents.
func (m *Memtable) Size() int64 {
	return m.sl.Size()
}

// ShouldFlush returns true when the memtable has exceeded its size threshold
// and should be flushed to disk as an SSTable.
func (m *Memtable) ShouldFlush() bool {
	return m.sl.Size() > m.MaxSize
}

func (m *Memtable) Set(key string, value []byte) {
	m.sl.Set(key, value)
}

func (m *Memtable) Delete(key string) {
	m.sl.Delete(key)
}

func (m *Memtable) Get(key string) ([]byte, bool) {
	return m.sl.Get(key)
}

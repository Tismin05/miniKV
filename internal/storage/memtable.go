package storage

import "sort"

// MemTable 定义 Engine 依赖的内存表能力。实现必须在冻结后保持只读。
type MemTable interface {
	Get(key string) (InternalEntry, bool)
	Set(entry InternalEntry) bool
	Entries() []InternalEntry
	Len() int
	SizeBytes() int
}

type mapMemTable struct {
	entries   map[string]InternalEntry
	sizeBytes int
}

func newMemTable() *mapMemTable {
	return &mapMemTable{entries: make(map[string]InternalEntry)}
}

func (m *mapMemTable) Get(key string) (InternalEntry, bool) {
	entry, ok := m.entries[key]
	entry.Value = append([]byte(nil), entry.Value...)
	return entry, ok
}

// Set 只接收能够赢过表内当前版本的条目，返回值表示内存视图是否发生变化。
func (m *mapMemTable) Set(entry InternalEntry) bool {
	current, exists := m.entries[entry.Key]
	if exists && compareVersions(entry, current) <= 0 {
		return false
	}
	if exists {
		m.sizeBytes -= entryFootprint(current)
	}
	entry.Value = append([]byte(nil), entry.Value...)
	m.entries[entry.Key] = entry
	m.sizeBytes += entryFootprint(entry)
	return true
}

func (m *mapMemTable) Entries() []InternalEntry {
	entries := make([]InternalEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		entry.Value = append([]byte(nil), entry.Value...)
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return entries
}

func (m *mapMemTable) Len() int       { return len(m.entries) }
func (m *mapMemTable) SizeBytes() int { return m.sizeBytes }

func entryFootprint(entry InternalEntry) int {
	return len(entry.Key) + len(entry.Value) + 8 + 8 + 1
}

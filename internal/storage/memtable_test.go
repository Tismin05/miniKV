package storage

import "testing"

func TestMemTableKeepsWinningVersionAndSortedSnapshot(t *testing.T) {
	table := newMemTable()
	table.Set(InternalEntry{Key: "b", Value: []byte("new"), Timestamp: 2, Type: ValuePut})
	table.Set(InternalEntry{Key: "b", Value: []byte("old"), Timestamp: 1, Type: ValuePut})
	table.Set(InternalEntry{Key: "a", Value: []byte("value"), Timestamp: 1, Type: ValuePut})
	entries := table.Entries()
	if len(entries) != 2 || entries[0].Key != "a" || entries[1].Key != "b" || string(entries[1].Value) != "new" {
		t.Fatalf("entries = %#v", entries)
	}
	entries[1].Value[0] = 'X'
	entry, _ := table.Get("b")
	if string(entry.Value) != "new" {
		t.Fatal("Entries exposed mutable MemTable storage")
	}
}

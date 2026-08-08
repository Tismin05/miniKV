package compaction

import (
	"path/filepath"
	"testing"

	"miniKV/internal/storage/sstable"
)

func TestMergeGeneratesDeterministicLatestVersions(t *testing.T) {
	dir := t.TempDir()
	writer := sstable.NewWriter(sstable.Options{})
	firstPath := filepath.Join(dir, "data_0.sst")
	secondPath := filepath.Join(dir, "data_1.sst")
	if _, err := writer.Write(firstPath, []sstable.Entry{
		{Key: "deleted", Value: []byte("old"), Timestamp: 1, Type: 1},
		{Key: "key", Value: []byte("alpha"), Timestamp: 2, Type: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(secondPath, []sstable.Entry{
		{Key: "deleted", Timestamp: 3, Type: 2},
		{Key: "key", Value: []byte("zulu"), Timestamp: 2, Type: 1},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := sstable.Open(firstPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sstable.Open(secondPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	merged, err := Merge([]Source{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 2 || merged[0].Key != "deleted" || merged[0].Type != 2 || merged[1].Key != "key" || string(merged[1].Value) != "zulu" {
		t.Fatalf("merged = %#v", merged)
	}
}

package compaction

import (
	"bytes"
	"fmt"
	"sort"

	"miniKV/internal/storage/sstable"
)

// Source 让 compaction 读取不可变输入，而不依赖 Engine 的 tableHandle。
type Source interface {
	Path() string
	Entries() ([]sstable.Entry, error)
}

// Merge 生成每个 key 的 LWW 胜出条目，输出可直接交给 SSTable Writer。
func Merge(inputs []Source) ([]sstable.Entry, error) {
	winners := make(map[string]sstable.Entry)
	for _, input := range inputs {
		entries, err := input.Entries()
		if err != nil {
			return nil, fmt.Errorf("read compaction input %s: %w", input.Path(), err)
		}
		for _, candidate := range entries {
			current, exists := winners[candidate.Key]
			if !exists || compareEntries(candidate, current) >= 0 {
				candidate.Value = append([]byte(nil), candidate.Value...)
				winners[candidate.Key] = candidate
			}
		}
	}
	keys := make([]string, 0, len(winners))
	for key := range winners {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	merged := make([]sstable.Entry, 0, len(keys))
	for _, key := range keys {
		merged = append(merged, winners[key])
	}
	return merged, nil
}

func compareEntries(candidate, current sstable.Entry) int {
	if candidate.Timestamp > current.Timestamp {
		return 1
	}
	if candidate.Timestamp < current.Timestamp {
		return -1
	}
	if candidate.Type == 2 && current.Type != 2 {
		return 1
	}
	if candidate.Type != 2 && current.Type == 2 {
		return -1
	}
	if candidate.Type == 1 {
		return bytes.Compare(candidate.Value, current.Value)
	}
	return 0
}

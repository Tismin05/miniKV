package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriterReaderBlockIndexBloomAndProperties(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data_0.sst")
	entries := testEntries(200)
	properties, err := NewWriter(Options{BlockSize: 256, RestartInterval: 4, BloomBitsPerKey: 12}).Write(path, entries)
	if err != nil {
		t.Fatal(err)
	}
	if properties.EntryCount != 200 || properties.SmallestKey != "key-000" || properties.LargestKey != "key-199" || properties.MinSequence != 1 || properties.MaxSequence != 200 {
		t.Fatalf("properties = %#v", properties)
	}
	reader, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if len(reader.index) < 2 {
		t.Fatalf("data block count = %d, want multiple blocks", len(reader.index))
	}
	before := reader.BlockReads()
	entry, found, err := reader.Get("key-123")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(entry.Value) != "value-123" || entry.Sequence != 124 {
		t.Fatalf("entry = %#v, found=%v", entry, found)
	}
	if reads := reader.BlockReads() - before; reads != 1 {
		t.Fatalf("point lookup read %d data blocks, want 1", reads)
	}

	// 在 key range 内寻找一个 Bloom 明确判定不存在的 key；此时不能读取 Data Block。
	missing := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("key-100-missing-%d", i)
		if !reader.bloom.mayContain(candidate) {
			missing = candidate
			break
		}
	}
	if missing == "" {
		t.Fatal("could not find Bloom-negative test key")
	}
	before = reader.BlockReads()
	if _, found, err := reader.Get(missing); err != nil || found {
		t.Fatalf("missing Get = found %v, error %v", found, err)
	}
	if reader.BlockReads() != before {
		t.Fatal("Bloom-negative lookup performed a data block read")
	}
}

func TestReaderUsesBlockCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data_0.sst")
	if _, err := NewWriter(Options{BlockSize: 128}).Write(path, testEntries(30)); err != nil {
		t.Fatal(err)
	}
	cache := &mapBlockCache{blocks: make(map[BlockID][]byte)}
	first, err := Open(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := first.Get("key-010"); err != nil || !found {
		t.Fatalf("first Get: found=%v err=%v", found, err)
	}
	if first.BlockReads() != 1 {
		t.Fatalf("first block reads = %d", first.BlockReads())
	}
	_ = first.Close()
	second, err := Open(path, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, found, err := second.Get("key-010"); err != nil || !found {
		t.Fatalf("cached Get: found=%v err=%v", found, err)
	}
	if second.BlockReads() != 0 {
		t.Fatalf("cached lookup read %d disk blocks", second.BlockReads())
	}
}

func TestReaderReportsCorruptDataBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data_0.sst")
	if _, err := NewWriter(Options{BlockSize: 128}).Write(path, testEntries(20)); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	handle := reader.index[0].Handle
	firstKey := "key-000"
	_ = reader.Close()
	file, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	byteAtOffset := []byte{0}
	if _, err := file.ReadAt(byteAtOffset, int64(handle.Offset+5)); err != nil {
		t.Fatal(err)
	}
	byteAtOffset[0] ^= 0xff
	if _, err := file.WriteAt(byteAtOffset, int64(handle.Offset+5)); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	reader, err = Open(path, nil)
	if err != nil {
		t.Fatalf("metadata should still open: %v", err)
	}
	defer reader.Close()
	_, _, err = reader.Get(firstKey)
	if !errors.Is(err, ErrCorruptBlock) {
		t.Fatalf("Get error = %v, want ErrCorruptBlock", err)
	}
}

func TestReaderOpensLegacyV2AndNewWriterMigratesToV3(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "data_0.sst")
	file, err := os.Create(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(legacyMagic[:]); err != nil {
		t.Fatal(err)
	}
	if err := writeLegacyRecord(file, Entry{Key: "legacy", Value: []byte("value"), Sequence: 7, Timestamp: 9, Type: 1}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	reader, err := Open(legacyPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := reader.Get("legacy")
	if err != nil || !found || string(entry.Value) != "value" || reader.Format() != 2 {
		t.Fatalf("legacy Get = %#v found=%v format=%d err=%v", entry, found, reader.Format(), err)
	}
	_ = reader.Close()

	v3Path := filepath.Join(dir, "data_1.sst")
	if _, err := NewWriter(Options{}).Write(v3Path, []Entry{entry}); err != nil {
		t.Fatal(err)
	}
	v3, err := Open(v3Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v3.Close()
	if v3.Format() != FormatVersion {
		t.Fatalf("migrated format = %d", v3.Format())
	}
}

func testEntries(count int) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		entries[i] = Entry{
			Key: fmt.Sprintf("key-%03d", i), Value: []byte(fmt.Sprintf("value-%03d", i)),
			Sequence: uint64(i + 1), Timestamp: int64(i + 1), Type: 1,
		}
	}
	return entries
}

type mapBlockCache struct {
	mu     sync.Mutex
	blocks map[BlockID][]byte
}

func (c *mapBlockCache) Get(id BlockID) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.blocks[id]
	return append([]byte(nil), data...), ok
}

func (c *mapBlockCache) Set(id BlockID, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[id] = append([]byte(nil), data...)
}

func writeLegacyRecord(file *os.File, entry Entry) error {
	header := make([]byte, 25)
	header[0] = entry.Type
	binary.BigEndian.PutUint64(header[1:9], uint64(entry.Timestamp))
	binary.BigEndian.PutUint64(header[9:17], entry.Sequence)
	binary.BigEndian.PutUint32(header[17:21], uint32(len(entry.Key)))
	binary.BigEndian.PutUint32(header[21:25], uint32(len(entry.Value)))
	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write([]byte(entry.Key)); err != nil {
		return err
	}
	_, err := file.Write(entry.Value)
	return err
}

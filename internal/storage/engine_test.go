package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"miniKV/internal/storage/sstable"
	storagewal "miniKV/internal/storage/wal"
)

func TestFlushCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := NewEngine(dir, Options{MaxMemTableEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	writes := []struct {
		key, value string
		ts         int64
	}{
		{"a,\n\x00", "first,value\n\x00", 1},
		{"二号键", "🙂\r\nvalue", 2},
		{"a,\n\x00", "new,<TOMBSTONE>\nvalue", 3},
		{"literal-tombstone", "<TOMBSTONE>", 4},
	}
	for _, item := range writes {
		if err := store.Put(item.key, item.value, item.ts); err != nil {
			t.Fatalf("Put(%q): %v", item.key, err)
		}
	}
	if err := store.Delete("deleted,\nkey", 5); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "data_*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("SSTable count after compaction = %d, want 1", len(files))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewKVStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	assertGet(t, reopened, "a,\n\x00", "new,<TOMBSTONE>\nvalue", 3)
	assertGet(t, reopened, "二号键", "🙂\r\nvalue", 2)
	assertGet(t, reopened, "literal-tombstone", "<TOMBSTONE>", 4)
	assertDeleted(t, reopened, "deleted,\nkey", 5)

	reader, err := sstable.Open(files[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Format() != sstable.FormatVersion {
		t.Fatalf("SSTable format = %d, want %d", reader.Format(), sstable.FormatVersion)
	}
}

func TestFlushDoesNotBlockWritesUntilBackpressureLimit(t *testing.T) {
	store, err := NewEngine(t.TempDir(), Options{MaxMemTableEntries: 1, MaxImmutableTables: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	originalWriter := store.writeTable
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store.writeTable = func(path string, entries []sstable.Entry) (sstable.Properties, error) {
		once.Do(func() { close(started) })
		<-release
		return originalWriter(path, entries)
	}
	if err := store.Put("first", "value", 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Flush Worker did not start")
	}
	// 第一个 Immutable 正在慢速落盘时，新写入仍能进入新的 Active。
	if err := store.Put("second", "value", 2); err != nil {
		t.Fatalf("write during flush: %v", err)
	}

	thirdDone := make(chan error, 1)
	go func() { thirdDone <- store.Put("third", "value", 3) }()
	select {
	case err := <-thirdDone:
		t.Fatalf("write bypassed immutable backpressure: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-thirdDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backpressured write did not resume")
	}
	assertGet(t, store, "first", "value", 1)
	assertGet(t, store, "second", "value", 2)
	assertGet(t, store, "third", "value", 3)
}

func TestFlushFailureRetainsImmutableAndRetries(t *testing.T) {
	store, err := NewEngine(t.TempDir(), Options{
		MaxMemTableEntries: 1, MaxImmutableTables: 1, FlushRetryInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	originalWriter := store.writeTable
	var attempts atomic.Int32
	store.writeTable = func(path string, entries []sstable.Entry) (sstable.Properties, error) {
		if attempts.Add(1) == 1 {
			return sstable.Properties{}, errors.New("injected flush failure")
		}
		return originalWriter(path, entries)
	}
	if err := store.Put("durable", "value", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush after retry: %v", err)
	}
	if attempts.Load() < 2 {
		t.Fatalf("flush attempts = %d, want retry", attempts.Load())
	}
	assertGet(t, store, "durable", "value", 1)
	files, err := filepath.Glob(filepath.Join(store.dataDir, "data_*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("published SSTables = %d, want exactly one", len(files))
	}
}

func TestStoresUseIsolatedDataDirectories(t *testing.T) {
	root := t.TempDir()
	first, err := NewKVStore(filepath.Join(root, "node-5001"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewKVStore(filepath.Join(root, "node-5002"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put("only-first", "one", 1); err != nil {
		t.Fatal(err)
	}
	if err := second.Put("only-second", "two", 1); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	first, _ = NewKVStore(filepath.Join(root, "node-5001"))
	defer first.Close()
	second, _ = NewKVStore(filepath.Join(root, "node-5002"))
	defer second.Close()
	assertGet(t, first, "only-first", "one", 1)
	if first.Get("only-second").Found {
		t.Fatal("first node read second node's SSTable")
	}
	assertGet(t, second, "only-second", "two", 1)
}

func TestCrashRecoveryFromSyncedWAL(t *testing.T) {
	if os.Getenv("MINIKV_CRASH_HELPER") == "1" {
		store, err := NewKVStore(os.Getenv("MINIKV_CRASH_DIR"))
		if err != nil {
			os.Exit(2)
		}
		if err := store.Put("crash,\nkey\x00", "durable,\nvalue\x00🙂", 99); err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryFromSyncedWAL$")
	cmd.Env = append(os.Environ(), "MINIKV_CRASH_HELPER=1", "MINIKV_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper failed: %v\n%s", err, output)
	}
	walFile, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := walFile.Write([]byte{storagewal.TypePut, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := walFile.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := walFile.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewKVStore(dir)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertGet(t, recovered, "crash,\nkey\x00", "durable,\nvalue\x00🙂", 99)
	if err := recovered.Put("after-recovery", "still works", 100); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	final, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	assertGet(t, final, "after-recovery", "still works", 100)
}

func TestRecoveryWithPublishedCompactionAndOldFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := NewEngine(dir, Options{MaxMemTableEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("other", "value", 2); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", "new", 3); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := sstable.NewWriter(sstable.Options{}).Write(filepath.Join(dir, "data_99.sst"), []sstable.Entry{
		{Key: "key", Value: []byte("new"), Timestamp: 3, Sequence: 3, Type: byte(ValuePut)},
		{Key: "other", Value: []byte("value"), Timestamp: 2, Sequence: 2, Type: byte(ValuePut)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	assertGet(t, recovered, "key", "new", 3)
	assertGet(t, recovered, "other", "value", 2)
	if err := recovered.Compact(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "data_*.sst"))
	if len(files) != 1 {
		t.Fatalf("SSTable count after resumed compaction = %d, want 1", len(files))
	}
}

func TestEngineReadsV2SSTableAndCompactsToV3(t *testing.T) {
	dir := t.TempDir()
	legacyPath := filepath.Join(dir, "data_0.sst")
	file, err := os.Create(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{'M', 'K', 'V', 'S', 'S', 'T', 2, 0}); err != nil {
		t.Fatal(err)
	}
	if err := writeV2Entry(file, InternalEntry{Key: "legacy", Value: []byte("value"), Sequence: 7, Timestamp: 9, Type: ValuePut}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewKVStore(dir)
	if err != nil {
		t.Fatalf("open v2 directory: %v", err)
	}
	assertGet(t, store, "legacy", "value", 9)
	if err := store.Put("new", "entry", 10); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "data_*.sst"))
	if len(files) != 1 {
		t.Fatalf("SSTable count = %d, want migrated compacted file", len(files))
	}
	reader, err := sstable.Open(files[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.Format() != sstable.FormatVersion {
		t.Fatalf("compacted format = %d, want %d", reader.Format(), sstable.FormatVersion)
	}
}

func TestLWWDeleteAndTypedReadResult(t *testing.T) {
	store, err := NewKVStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put("key", "old", 10); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("key", 20); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", "stale", 15); err != nil {
		t.Fatal(err)
	}
	assertDeleted(t, store, "key", 20)
	if err := store.Put("literal", "<TOMBSTONE>", 30); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "literal", "<TOMBSTONE>", 30)
}

func TestVersionConflictsAreDeterministic(t *testing.T) {
	store, err := NewKVStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_ = store.Put("key", "alpha", 10)
	_ = store.Put("key", "zulu", 10)
	assertGet(t, store, "key", "zulu", 10)
	_ = store.Delete("key", 10)
	assertDeleted(t, store, "key", 10)
}

func TestEmptyKeyAndValueAreValid(t *testing.T) {
	store, err := NewKVStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Put("", "", 1); err != nil {
		t.Fatal(err)
	}
	assertGet(t, store, "", "", 1)
}

func TestConcurrentReadWriteAndFlush(t *testing.T) {
	store, err := NewEngine(t.TempDir(), Options{MaxMemTableEntries: 8, MaxImmutableTables: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key-%d-%d", worker, i)
				if err := store.Put(key, "value", int64(worker*100+i+1)); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				_ = store.Get(key)
			}
		}()
	}
	group.Wait()
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
}

func assertGet(t *testing.T, store *Engine, key, wantValue string, wantTimestamp int64) {
	t.Helper()
	result, err := store.GetWithError(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if !result.Found || result.Deleted || string(result.Value) != wantValue || result.Timestamp != wantTimestamp {
		t.Fatalf("Get(%q) = %#v, want (%q, %d)", key, result, wantValue, wantTimestamp)
	}
}

func assertDeleted(t *testing.T, store *Engine, key string, wantTimestamp int64) {
	t.Helper()
	result := store.Get(key)
	if result.Found || !result.Deleted || result.Timestamp != wantTimestamp {
		t.Fatalf("Get(%q) = %#v, want deleted version %d", key, result, wantTimestamp)
	}
}

func BenchmarkPutAndGet(b *testing.B) {
	store, err := NewKVStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Put("key", "value", int64(i+1)); err != nil {
			b.Fatal(err)
		}
		if result := store.Get("key"); !result.Found {
			b.Fatalf("Get = %#v", result)
		}
	}
}

func writeV2Entry(file *os.File, entry InternalEntry) error {
	header := make([]byte, 25)
	header[0] = byte(entry.Type)
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

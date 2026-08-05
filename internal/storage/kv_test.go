package storage

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRecordEncodingSupportsArbitraryStrings(t *testing.T) {
	want := diskRecord{
		key:       "key,with\nnewlines\x00and-中文",
		value:     "value,\r\n\x00<ＴＯＭＢＳＴＯＮＥ>🙂",
		timestamp: -42,
	}
	var encoded bytes.Buffer
	size, err := writeRecord(&encoded, want)
	if err != nil {
		t.Fatalf("writeRecord: %v", err)
	}
	got, gotSize, err := readRecord(&encoded)
	if err != nil {
		t.Fatalf("readRecord: %v", err)
	}
	if gotSize != size {
		t.Fatalf("record size = %d, want %d", gotSize, size)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded record = %#v, want %#v", got, want)
	}
}

func TestFlushCompactionAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.maxMemSize = 1

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
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened store: %v", err)
		}
	}()
	assertGet(t, reopened, "a,\n\x00", "new,<TOMBSTONE>\nvalue", 3)
	assertGet(t, reopened, "二号键", "🙂\r\nvalue", 2)
	assertGet(t, reopened, "literal-tombstone", "<TOMBSTONE>", 4)
	assertGet(t, reopened, "deleted,\nkey", "<TOMBSTONE>", 5)
	if reopened.index["literal-tombstone"].Deleted {
		t.Fatal("literal tombstone value was decoded as a delete operation")
	}
	if !reopened.index["deleted,\nkey"].Deleted {
		t.Fatal("delete operation lost its explicit tombstone bit")
	}
}

func TestStoresUseIsolatedDataDirectories(t *testing.T) {
	root := t.TempDir()
	firstDir := filepath.Join(root, "node-5001")
	secondDir := filepath.Join(root, "node-5002")
	first, err := NewKVStore(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewKVStore(secondDir)
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

	first, err = NewKVStore(firstDir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err = NewKVStore(secondDir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	assertGet(t, first, "only-first", "one", 1)
	if _, _, ok := first.Get("only-second"); ok {
		t.Fatal("first node read second node's SSTable")
	}
	assertGet(t, second, "only-second", "two", 1)
	if _, _, ok := second.Get("only-first"); ok {
		t.Fatal("second node read first node's SSTable")
	}
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
		// Deliberately bypass Close to emulate sudden process termination.
		os.Exit(0)
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryFromSyncedWAL$")
	cmd.Env = append(os.Environ(), "MINIKV_CRASH_HELPER=1", "MINIKV_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash helper failed: %v\n%s", err, output)
	}

	// Also emulate a torn next append. Recovery must keep the valid prefix and
	// truncate this incomplete tail before accepting new writes.
	wal, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Write([]byte{opPut, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewKVStore(dir)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertGet(t, recovered, "crash,\nkey\x00", "durable,\nvalue\x00🙂", 99)
	if err := recovered.Put("after-recovery", "still works", 100); err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	final, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	assertGet(t, final, "crash,\nkey\x00", "durable,\nvalue\x00🙂", 99)
	assertGet(t, final, "after-recovery", "still works", 100)
}

func TestRecoveryAfterCompactionPublishBeforeOldFileRemoval(t *testing.T) {
	dir := t.TempDir()
	store, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.maxMemSize = 1
	if err := store.Put("key", "old", 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("other", "value", 2); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", "new", 3); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Model the exact durable state after compaction's rename+directory fsync,
	// but before it starts deleting old generations.
	temp, err := os.CreateTemp(dir, ".compact-test-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAll(temp, sstMagic[:]); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"key", "other"} {
		record, err := readRecordAt(store.index[key])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writeRecord(temp, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := temp.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := temp.Close(); err != nil {
		t.Fatal(err)
	}
	published := filepath.Join(dir, "data_3.sst")
	if err := os.Rename(temp.Name(), published); err != nil {
		t.Fatal(err)
	}
	if err := syncDir(dir); err != nil {
		t.Fatal(err)
	}

	recovered, err := NewKVStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	assertGet(t, recovered, "key", "new", 3)
	assertGet(t, recovered, "other", "value", 2)
	if err := recovered.Compact(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "data_*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("SSTable count after resumed compaction = %d, want 1", len(files))
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadRecordRejectsTruncatedPayload(t *testing.T) {
	var encoded bytes.Buffer
	if _, err := writeRecord(&encoded, diskRecord{key: "key", value: "value", timestamp: 1}); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	_, _, err := readRecord(bytes.NewReader(data[:len(data)-1]))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func assertGet(t *testing.T, store *KVStore, key, wantValue string, wantTimestamp int64) {
	t.Helper()
	value, timestamp, ok := store.Get(key)
	if !ok {
		t.Fatalf("Get(%q) did not find key", key)
	}
	if value != wantValue || timestamp != wantTimestamp {
		t.Fatalf("Get(%q) = (%q, %d), want (%q, %d)", key, value, timestamp, wantValue, wantTimestamp)
	}
}

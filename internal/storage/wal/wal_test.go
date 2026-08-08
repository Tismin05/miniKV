package wal

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRecordCodecSupportsArbitraryBytes(t *testing.T) {
	want := Record{Key: "key,\n\x00中文", Value: []byte("value\x00🙂"), Sequence: 7, Timestamp: -42, Type: TypePut}
	var encoded bytes.Buffer
	size, err := writeRecord(&encoded, want)
	if err != nil {
		t.Fatal(err)
	}
	got, gotSize, err := readRecord(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	if gotSize != size || !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded = %#v (%d), want %#v (%d)", got, gotSize, want, size)
	}
}

func TestOpenRecoversSegmentsAndTruncatesActiveTail(t *testing.T) {
	dir := t.TempDir()
	log, recovery, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovery.Active) != 0 {
		t.Fatal("new WAL unexpectedly recovered records")
	}
	first := Record{Key: "first", Value: []byte("one"), Sequence: 1, Timestamp: 1, Type: TypePut}
	if err := log.Append(first); err != nil {
		t.Fatal(err)
	}
	sealed, err := log.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	second := Record{Key: "second", Sequence: 2, Timestamp: 2, Type: TypeDelete}
	if err := log.Append(second); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(dir, activeName)
	file, err := os.OpenFile(activePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{TypePut, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	reopened, recovered, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(recovered.Sealed) != 1 || !reflect.DeepEqual(recovered.Sealed[0].Records, []Record{first}) {
		t.Fatalf("sealed recovery = %#v", recovered.Sealed)
	}
	if !reflect.DeepEqual(recovered.Active, []Record{second}) {
		t.Fatalf("active recovery = %#v", recovered.Active)
	}
	info, err := os.Stat(activePath)
	if err != nil {
		t.Fatal(err)
	}
	wantSize := int64(fileHeaderSize + recordHeaderSize + len(second.Key) + len(second.Value))
	if info.Size() != wantSize {
		t.Fatalf("active WAL size = %d, want truncated %d", info.Size(), wantSize)
	}
	if err := reopened.RemoveSegment(sealed); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sealed); !os.IsNotExist(err) {
		t.Fatalf("sealed WAL still exists: %v", err)
	}
}

func TestReadRecordRejectsTruncatedPayload(t *testing.T) {
	var encoded bytes.Buffer
	_, _ = writeRecord(&encoded, Record{Key: "key", Value: []byte("value"), Type: TypePut})
	data := encoded.Bytes()
	_, _, err := readRecord(bytes.NewReader(data[:len(data)-1]))
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("error = %v, want io.ErrUnexpectedEOF", err)
	}
}

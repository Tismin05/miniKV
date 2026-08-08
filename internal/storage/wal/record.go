package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	fileHeaderSize   = 8
	recordHeaderSize = 25
	maxRecordBytes   = uint64(^uint32(0))

	TypePut    byte = 1
	TypeDelete byte = 2
)

var fileMagic = [fileHeaderSize]byte{'M', 'K', 'V', 'W', 'A', 'L', 2, 0}

// Record 是 WAL 的稳定磁盘模型。WAL 不依赖 Engine 的内存结构，便于独立恢复。
type Record struct {
	Key       string
	Value     []byte
	Sequence  uint64
	Timestamp int64
	Type      byte
}

func writeRecord(w io.Writer, record Record) (int, error) {
	keyLen, valueLen := uint64(len(record.Key)), uint64(len(record.Value))
	if keyLen > maxRecordBytes || valueLen > maxRecordBytes {
		return 0, errors.New("key or value exceeds uint32 encoding limit")
	}
	if record.Type != TypePut && record.Type != TypeDelete {
		return 0, fmt.Errorf("unknown operation %d", record.Type)
	}
	header := make([]byte, recordHeaderSize)
	header[0] = record.Type
	binary.BigEndian.PutUint64(header[1:9], uint64(record.Timestamp))
	binary.BigEndian.PutUint64(header[9:17], record.Sequence)
	binary.BigEndian.PutUint32(header[17:21], uint32(keyLen))
	binary.BigEndian.PutUint32(header[21:25], uint32(valueLen))
	if err := writeAll(w, header); err != nil {
		return 0, err
	}
	if err := writeAll(w, []byte(record.Key)); err != nil {
		return 0, err
	}
	if err := writeAll(w, record.Value); err != nil {
		return 0, err
	}
	return recordHeaderSize + len(record.Key) + len(record.Value), nil
}

// readRecord 对截断的头部或载荷返回 io.ErrUnexpectedEOF，供恢复逻辑丢弃损坏尾部。
func readRecord(r io.Reader) (Record, int, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, 0, err
	}
	if header[0] != TypePut && header[0] != TypeDelete {
		return Record{}, 0, fmt.Errorf("unknown operation %d", header[0])
	}
	keyLen := uint64(binary.BigEndian.Uint32(header[17:21]))
	valueLen := uint64(binary.BigEndian.Uint32(header[21:25]))
	total := uint64(recordHeaderSize) + keyLen + valueLen
	if total > uint64(int(^uint(0)>>1)) {
		return Record{}, 0, errors.New("record is too large for this platform")
	}
	payload := make([]byte, int(keyLen+valueLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return Record{}, 0, err
	}
	return Record{
		Key:       string(payload[:keyLen]),
		Value:     append([]byte(nil), payload[keyLen:]...),
		Sequence:  binary.BigEndian.Uint64(header[9:17]),
		Timestamp: int64(binary.BigEndian.Uint64(header[1:9])),
		Type:      header[0],
	}, int(total), nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

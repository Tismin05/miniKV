package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	FormatVersion   = 3
	fileHeaderSize  = 8
	footerSize      = 52
	entryHeaderSize = 29
)

var (
	fileMagic   = [fileHeaderSize]byte{'M', 'K', 'V', 'S', 'S', 'T', 3, 0}
	footerMagic = [8]byte{'M', 'K', 'V', 'F', 'O', 'O', 'T', 3}
	crcTable    = crc32.MakeTable(crc32.Castagnoli)
)

// Entry 是 SSTable 的独立持久化模型，Type 的 1/2 分别表示 Put/Delete。
type Entry struct {
	Key       string
	Value     []byte
	Sequence  uint64
	Timestamp int64
	Type      byte
}

// Options 控制新格式的数据块、restart point 和 Bloom Filter。
type Options struct {
	BlockSize       int
	RestartInterval int
	BloomBitsPerKey int
}

func (o Options) normalized() Options {
	if o.BlockSize <= 0 {
		o.BlockSize = 4 << 10
	}
	if o.RestartInterval <= 0 {
		o.RestartInterval = 16
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = 10
	}
	return o
}

type blockHandle struct {
	Offset uint64
	Length uint32
}

type indexEntry struct {
	LastKey string
	Handle  blockHandle
}

// Writer 将排序条目写入临时文件，并通过 fsync、rename、目录 fsync 原子发布。
type Writer struct {
	options Options
}

func NewWriter(options Options) *Writer {
	return &Writer{options: options.normalized()}
}

// Write 要求同一个 key 只出现一次；Writer 自行排序，不把排序责任泄漏给 Engine。
func (w *Writer) Write(finalPath string, entries []Entry) (Properties, error) {
	entries = cloneEntries(entries)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Key == entries[i].Key {
			return Properties{}, fmt.Errorf("duplicate key %q", entries[i].Key)
		}
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return Properties{}, fmt.Errorf("create SSTable directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(finalPath), ".sstable-*.tmp")
	if err != nil {
		return Properties{}, fmt.Errorf("create temporary SSTable: %w", err)
	}
	tempPath := temp.Name()
	published := false
	defer func() {
		if !published {
			_ = temp.Close()
			_ = os.Remove(tempPath)
		}
	}()

	if err := writeAll(temp, fileMagic[:]); err != nil {
		return Properties{}, fmt.Errorf("write SSTable header: %w", err)
	}
	index, properties, err := w.writeDataBlocks(temp, entries)
	if err != nil {
		return Properties{}, err
	}
	indexHandle, err := writeChecksummedBlock(temp, encodeIndex(index))
	if err != nil {
		return Properties{}, fmt.Errorf("write index block: %w", err)
	}
	keys := make([]string, len(entries))
	for i := range entries {
		keys[i] = entries[i].Key
	}
	bloomHandle, err := writeChecksummedBlock(temp, buildBloom(keys, w.options.BloomBitsPerKey))
	if err != nil {
		return Properties{}, fmt.Errorf("write Bloom Filter block: %w", err)
	}
	propertiesHandle, err := writeChecksummedBlock(temp, encodeProperties(properties))
	if err != nil {
		return Properties{}, fmt.Errorf("write properties block: %w", err)
	}
	if err := writeFooter(temp, indexHandle, bloomHandle, propertiesHandle); err != nil {
		return Properties{}, fmt.Errorf("write footer: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return Properties{}, fmt.Errorf("sync temporary SSTable: %w", err)
	}
	if err := temp.Close(); err != nil {
		return Properties{}, fmt.Errorf("close temporary SSTable: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return Properties{}, fmt.Errorf("publish SSTable: %w", err)
	}
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return Properties{}, fmt.Errorf("sync SSTable directory: %w", err)
	}
	published = true
	return properties, nil
}

func (w *Writer) writeDataBlocks(file *os.File, entries []Entry) ([]indexEntry, Properties, error) {
	properties := propertiesFor(entries)
	var index []indexEntry
	for start := 0; start < len(entries); {
		builder := newDataBlockBuilder(w.options.RestartInterval)
		end := start
		for end < len(entries) {
			encodedSize := builder.estimatedEntrySize(entries[end])
			if builder.count > 0 && len(builder.data)+encodedSize > w.options.BlockSize {
				break
			}
			builder.add(entries[end])
			end++
		}
		payload := builder.finish()
		handle, err := writeChecksummedBlock(file, payload)
		if err != nil {
			return nil, Properties{}, fmt.Errorf("write data block: %w", err)
		}
		index = append(index, indexEntry{LastKey: entries[end-1].Key, Handle: handle})
		start = end
	}
	return index, properties, nil
}

type dataBlockBuilder struct {
	data            []byte
	restarts        []uint32
	previousKey     string
	count           int
	restartInterval int
}

func newDataBlockBuilder(restartInterval int) *dataBlockBuilder {
	return &dataBlockBuilder{restartInterval: restartInterval}
}

func (b *dataBlockBuilder) estimatedEntrySize(entry Entry) int {
	shared := 0
	if b.count%b.restartInterval != 0 {
		shared = sharedPrefix(b.previousKey, entry.Key)
	}
	return entryHeaderSize + len(entry.Key) - shared + len(entry.Value) + 4
}

func (b *dataBlockBuilder) add(entry Entry) {
	shared := 0
	if b.count%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.data)))
	} else {
		shared = sharedPrefix(b.previousKey, entry.Key)
	}
	unshared := entry.Key[shared:]
	header := make([]byte, entryHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(shared))
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(unshared)))
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(entry.Value)))
	binary.LittleEndian.PutUint64(header[12:20], entry.Sequence)
	binary.LittleEndian.PutUint64(header[20:28], uint64(entry.Timestamp))
	header[28] = entry.Type
	b.data = append(b.data, header...)
	b.data = append(b.data, unshared...)
	b.data = append(b.data, entry.Value...)
	b.previousKey = entry.Key
	b.count++
}

func (b *dataBlockBuilder) finish() []byte {
	for _, restart := range b.restarts {
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], restart)
		b.data = append(b.data, encoded[:]...)
	}
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(b.restarts)))
	b.data = append(b.data, count[:]...)
	return b.data
}

func encodeIndex(index []indexEntry) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, uint32(len(index)))
	for _, item := range index {
		start := len(data)
		data = append(data, make([]byte, 4+len(item.LastKey)+8+4)...)
		binary.LittleEndian.PutUint32(data[start:start+4], uint32(len(item.LastKey)))
		copy(data[start+4:], item.LastKey)
		position := start + 4 + len(item.LastKey)
		binary.LittleEndian.PutUint64(data[position:position+8], item.Handle.Offset)
		binary.LittleEndian.PutUint32(data[position+8:position+12], item.Handle.Length)
	}
	return data
}

func encodeProperties(properties Properties) []byte {
	data := make([]byte, 4+len(properties.SmallestKey)+4+len(properties.LargestKey)+8+8+8)
	position := 0
	binary.LittleEndian.PutUint32(data[position:position+4], uint32(len(properties.SmallestKey)))
	position += 4
	copy(data[position:], properties.SmallestKey)
	position += len(properties.SmallestKey)
	binary.LittleEndian.PutUint32(data[position:position+4], uint32(len(properties.LargestKey)))
	position += 4
	copy(data[position:], properties.LargestKey)
	position += len(properties.LargestKey)
	binary.LittleEndian.PutUint64(data[position:position+8], properties.MinSequence)
	position += 8
	binary.LittleEndian.PutUint64(data[position:position+8], properties.MaxSequence)
	position += 8
	binary.LittleEndian.PutUint64(data[position:position+8], properties.EntryCount)
	return data
}

func writeChecksummedBlock(file *os.File, payload []byte) (blockHandle, error) {
	offset, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return blockHandle{}, err
	}
	if err := writeAll(file, payload); err != nil {
		return blockHandle{}, err
	}
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], crc32.Checksum(payload, crcTable))
	if err := writeAll(file, checksum[:]); err != nil {
		return blockHandle{}, err
	}
	length := len(payload) + len(checksum)
	if uint64(length) > uint64(^uint32(0)) {
		return blockHandle{}, errors.New("block exceeds uint32 encoding limit")
	}
	return blockHandle{Offset: uint64(offset), Length: uint32(length)}, nil
}

func writeFooter(file *os.File, index, bloom, properties blockHandle) error {
	footer := make([]byte, footerSize)
	copy(footer[:8], footerMagic[:])
	binary.LittleEndian.PutUint32(footer[8:12], FormatVersion)
	putHandle(footer[12:24], index)
	putHandle(footer[24:36], bloom)
	putHandle(footer[36:48], properties)
	binary.LittleEndian.PutUint32(footer[48:52], crc32.Checksum(footer[:48], crcTable))
	return writeAll(file, footer)
}

func putHandle(target []byte, handle blockHandle) {
	binary.LittleEndian.PutUint64(target[:8], handle.Offset)
	binary.LittleEndian.PutUint32(target[8:12], handle.Length)
}

func propertiesFor(entries []Entry) Properties {
	properties := Properties{EntryCount: uint64(len(entries))}
	if len(entries) == 0 {
		return properties
	}
	properties.SmallestKey = entries[0].Key
	properties.LargestKey = entries[len(entries)-1].Key
	properties.MinSequence = entries[0].Sequence
	properties.MaxSequence = entries[0].Sequence
	for _, entry := range entries[1:] {
		if entry.Sequence < properties.MinSequence {
			properties.MinSequence = entry.Sequence
		}
		if entry.Sequence > properties.MaxSequence {
			properties.MaxSequence = entry.Sequence
		}
	}
	return properties
}

func cloneEntries(entries []Entry) []Entry {
	cloned := make([]Entry, len(entries))
	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Value = append([]byte(nil), entry.Value...)
	}
	return cloned
}

func sharedPrefix(a, b string) int {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	i := 0
	for i < limit && a[i] == b[i] {
		i++
	}
	return i
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

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

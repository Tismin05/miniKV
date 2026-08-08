package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync/atomic"
)

var (
	legacyMagic     = [fileHeaderSize]byte{'M', 'K', 'V', 'S', 'S', 'T', 2, 0}
	ErrCorruptBlock = errors.New("corrupt SSTable block")
)

// Properties 保存无需扫描 Data Block 即可获得的表级元数据。
type Properties struct {
	SmallestKey string
	LargestKey  string
	MinSequence uint64
	MaxSequence uint64
	EntryCount  uint64
}

// BlockID 唯一标识一个不可变文件中的数据块。
type BlockID struct {
	Path   string
	Offset uint64
}

// BlockCache 是 Reader 的最小缓存契约；Engine 可按容量策略提供具体实现。
type BlockCache interface {
	Get(BlockID) ([]byte, bool)
	Set(BlockID, []byte)
}

// Reader 打开时只读取 Footer、Index、Bloom 和 Properties；点查按需读取一个块。
type Reader struct {
	path       string
	file       *os.File
	fileSize   int64
	format     int
	index      []indexEntry
	bloom      bloomFilter
	properties Properties
	cache      BlockCache
	blockReads atomic.Uint64
	legacy     map[string]Entry
}

func Open(path string, cache BlockCache) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open SSTable %s: %w", path, err)
	}
	fail := true
	defer func() {
		if fail {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat SSTable %s: %w", path, err)
	}
	header := make([]byte, fileHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("read SSTable header %s: %w", path, err)
	}
	reader := &Reader{path: path, file: file, fileSize: info.Size(), cache: cache}
	switch string(header) {
	case string(fileMagic[:]):
		reader.format = FormatVersion
		if err := reader.readMetadata(); err != nil {
			return nil, err
		}
	case string(legacyMagic[:]):
		reader.format = 2
		if err := reader.readLegacy(); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unsupported or corrupt SSTable format")
	}
	fail = false
	return reader, nil
}

func (r *Reader) Path() string           { return r.path }
func (r *Reader) Format() int            { return r.format }
func (r *Reader) Properties() Properties { return r.properties }
func (r *Reader) BlockReads() uint64     { return r.blockReads.Load() }
func (r *Reader) Close() error           { return r.file.Close() }

// Get 先检查 key range 和 Bloom Filter，再通过 Block Index 定位至多一个数据块。
func (r *Reader) Get(key string) (Entry, bool, error) {
	if r.format == 2 {
		entry, ok := r.legacy[key]
		entry.Value = append([]byte(nil), entry.Value...)
		return entry, ok, nil
	}
	if r.properties.EntryCount == 0 || key < r.properties.SmallestKey || key > r.properties.LargestKey {
		return Entry{}, false, nil
	}
	if !r.bloom.mayContain(key) {
		return Entry{}, false, nil
	}
	position := sort.Search(len(r.index), func(i int) bool { return r.index[i].LastKey >= key })
	if position == len(r.index) {
		return Entry{}, false, nil
	}
	payload, err := r.readDataBlock(r.index[position].Handle)
	if err != nil {
		return Entry{}, false, err
	}
	entries, err := decodeDataBlock(payload)
	if err != nil {
		return Entry{}, false, fmt.Errorf("%w at offset %d: %v", ErrCorruptBlock, r.index[position].Handle.Offset, err)
	}
	entryPosition := sort.Search(len(entries), func(i int) bool { return entries[i].Key >= key })
	if entryPosition == len(entries) || entries[entryPosition].Key != key {
		return Entry{}, false, nil
	}
	return entries[entryPosition], true, nil
}

// Entries 按 key 顺序遍历所有块，供恢复校验和 compaction 使用。
func (r *Reader) Entries() ([]Entry, error) {
	if r.format == 2 {
		entries := make([]Entry, 0, len(r.legacy))
		for _, entry := range r.legacy {
			entry.Value = append([]byte(nil), entry.Value...)
			entries = append(entries, entry)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
		return entries, nil
	}
	entries := make([]Entry, 0, r.properties.EntryCount)
	for _, item := range r.index {
		payload, err := r.readDataBlock(item.Handle)
		if err != nil {
			return nil, err
		}
		decoded, err := decodeDataBlock(payload)
		if err != nil {
			return nil, fmt.Errorf("%w at offset %d: %v", ErrCorruptBlock, item.Handle.Offset, err)
		}
		entries = append(entries, decoded...)
	}
	return entries, nil
}

func (r *Reader) readMetadata() error {
	if r.fileSize < fileHeaderSize+footerSize {
		return errors.New("SSTable is too small for footer")
	}
	footer := make([]byte, footerSize)
	if _, err := r.file.ReadAt(footer, r.fileSize-footerSize); err != nil {
		return fmt.Errorf("read SSTable footer: %w", err)
	}
	if string(footer[:8]) != string(footerMagic[:]) || binary.LittleEndian.Uint32(footer[8:12]) != FormatVersion {
		return errors.New("unsupported or corrupt SSTable footer")
	}
	if crc32.Checksum(footer[:48], crcTable) != binary.LittleEndian.Uint32(footer[48:52]) {
		return fmt.Errorf("%w: footer checksum mismatch", ErrCorruptBlock)
	}
	indexHandle := getHandle(footer[12:24])
	bloomHandle := getHandle(footer[24:36])
	propertiesHandle := getHandle(footer[36:48])
	for _, handle := range []blockHandle{indexHandle, bloomHandle, propertiesHandle} {
		if err := r.validateHandle(handle); err != nil {
			return err
		}
	}
	indexPayload, err := r.readChecksummed(indexHandle, false)
	if err != nil {
		return fmt.Errorf("read index block: %w", err)
	}
	r.index, err = decodeIndex(indexPayload)
	if err != nil {
		return fmt.Errorf("decode index block: %w", err)
	}
	for _, item := range r.index {
		if err := r.validateHandle(item.Handle); err != nil {
			return err
		}
	}
	bloomPayload, err := r.readChecksummed(bloomHandle, false)
	if err != nil {
		return fmt.Errorf("read Bloom Filter block: %w", err)
	}
	if r.bloom, _ = decodeBloom(bloomPayload); len(bloomPayload) > 0 && len(r.bloom.bits) == 0 {
		return fmt.Errorf("%w: invalid Bloom Filter", ErrCorruptBlock)
	}
	propertiesPayload, err := r.readChecksummed(propertiesHandle, false)
	if err != nil {
		return fmt.Errorf("read properties block: %w", err)
	}
	r.properties, err = decodeProperties(propertiesPayload)
	if err != nil {
		return fmt.Errorf("decode properties block: %w", err)
	}
	return nil
}

func (r *Reader) readDataBlock(handle blockHandle) ([]byte, error) {
	id := BlockID{Path: r.path, Offset: handle.Offset}
	if r.cache != nil {
		if data, ok := r.cache.Get(id); ok {
			return append([]byte(nil), data...), nil
		}
	}
	payload, err := r.readChecksummed(handle, true)
	if err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.Set(id, append([]byte(nil), payload...))
	}
	return payload, nil
}

func (r *Reader) readChecksummed(handle blockHandle, countDataRead bool) ([]byte, error) {
	if handle.Length < 4 {
		return nil, fmt.Errorf("%w: block is shorter than checksum", ErrCorruptBlock)
	}
	data := make([]byte, handle.Length)
	if _, err := r.file.ReadAt(data, int64(handle.Offset)); err != nil {
		return nil, fmt.Errorf("read block at offset %d: %w", handle.Offset, err)
	}
	if countDataRead {
		r.blockReads.Add(1)
	}
	payload := data[:len(data)-4]
	want := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(payload, crcTable) != want {
		return nil, fmt.Errorf("%w at offset %d: checksum mismatch", ErrCorruptBlock, handle.Offset)
	}
	return payload, nil
}

func (r *Reader) validateHandle(handle blockHandle) error {
	end := handle.Offset + uint64(handle.Length)
	if handle.Offset < fileHeaderSize || end < handle.Offset || end > uint64(r.fileSize-footerSize) {
		return fmt.Errorf("%w: block handle outside file", ErrCorruptBlock)
	}
	return nil
}

func getHandle(data []byte) blockHandle {
	return blockHandle{Offset: binary.LittleEndian.Uint64(data[:8]), Length: binary.LittleEndian.Uint32(data[8:12])}
}

func decodeIndex(data []byte) ([]indexEntry, error) {
	if len(data) < 4 {
		return nil, errors.New("missing index count")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	position := 4
	index := make([]indexEntry, 0, count)
	for i := 0; i < count; i++ {
		if position+4 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		keyLen := int(binary.LittleEndian.Uint32(data[position : position+4]))
		position += 4
		if keyLen < 0 || position+keyLen+12 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		key := string(data[position : position+keyLen])
		position += keyLen
		handle := getHandle(data[position : position+12])
		position += 12
		if len(index) > 0 && index[len(index)-1].LastKey >= key {
			return nil, errors.New("index keys are not strictly ordered")
		}
		index = append(index, indexEntry{LastKey: key, Handle: handle})
	}
	if position != len(data) {
		return nil, errors.New("trailing index bytes")
	}
	return index, nil
}

func decodeProperties(data []byte) (Properties, error) {
	readString := func(position *int) (string, error) {
		if *position+4 > len(data) {
			return "", io.ErrUnexpectedEOF
		}
		length := int(binary.LittleEndian.Uint32(data[*position : *position+4]))
		*position += 4
		if length < 0 || *position+length > len(data) {
			return "", io.ErrUnexpectedEOF
		}
		value := string(data[*position : *position+length])
		*position += length
		return value, nil
	}
	position := 0
	smallest, err := readString(&position)
	if err != nil {
		return Properties{}, err
	}
	largest, err := readString(&position)
	if err != nil {
		return Properties{}, err
	}
	if position+24 != len(data) {
		return Properties{}, errors.New("invalid properties length")
	}
	return Properties{
		SmallestKey: smallest,
		LargestKey:  largest,
		MinSequence: binary.LittleEndian.Uint64(data[position : position+8]),
		MaxSequence: binary.LittleEndian.Uint64(data[position+8 : position+16]),
		EntryCount:  binary.LittleEndian.Uint64(data[position+16 : position+24]),
	}, nil
}

func decodeDataBlock(data []byte) ([]Entry, error) {
	if len(data) < 4 {
		return nil, errors.New("missing restart count")
	}
	restartCount := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	if restartCount <= 0 || restartCount > (len(data)-4)/4 {
		return nil, errors.New("invalid restart count")
	}
	restartStart := len(data) - 4 - restartCount*4
	previousRestart := uint32(0)
	for i := 0; i < restartCount; i++ {
		offset := binary.LittleEndian.Uint32(data[restartStart+i*4 : restartStart+i*4+4])
		if int(offset) >= restartStart || (i > 0 && offset <= previousRestart) {
			return nil, errors.New("invalid restart offset")
		}
		previousRestart = offset
	}
	position := 0
	previousKey := ""
	var entries []Entry
	for position < restartStart {
		if position+entryHeaderSize > restartStart {
			return nil, io.ErrUnexpectedEOF
		}
		header := data[position : position+entryHeaderSize]
		position += entryHeaderSize
		shared := int(binary.LittleEndian.Uint32(header[0:4]))
		unshared := int(binary.LittleEndian.Uint32(header[4:8]))
		valueLen := int(binary.LittleEndian.Uint32(header[8:12]))
		if shared > len(previousKey) || unshared < 0 || valueLen < 0 || position+unshared+valueLen > restartStart {
			return nil, errors.New("invalid prefix-compressed entry")
		}
		key := previousKey[:shared] + string(data[position:position+unshared])
		position += unshared
		value := append([]byte(nil), data[position:position+valueLen]...)
		position += valueLen
		if len(entries) > 0 && entries[len(entries)-1].Key >= key {
			return nil, errors.New("data block keys are not strictly ordered")
		}
		entries = append(entries, Entry{
			Key: key, Value: value,
			Sequence:  binary.LittleEndian.Uint64(header[12:20]),
			Timestamp: int64(binary.LittleEndian.Uint64(header[20:28])), Type: header[28],
		})
		previousKey = key
	}
	return entries, nil
}

// readLegacy 是 v2 升级策略：旧 SSTable 可读，新 flush/compaction 统一写 v3。
func (r *Reader) readLegacy() error {
	r.legacy = make(map[string]Entry)
	for {
		entry, err := readLegacyEntry(r.file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read legacy SSTable %s: %w", r.path, err)
		}
		r.legacy[entry.Key] = entry
	}
	entries, _ := r.Entries()
	r.properties = propertiesFor(entries)
	return nil
}

func readLegacyEntry(reader io.Reader) (Entry, error) {
	header := make([]byte, 25)
	if _, err := io.ReadFull(reader, header); err != nil {
		return Entry{}, err
	}
	if header[0] != 1 && header[0] != 2 {
		return Entry{}, fmt.Errorf("unknown operation %d", header[0])
	}
	keyLen := int(binary.BigEndian.Uint32(header[17:21]))
	valueLen := int(binary.BigEndian.Uint32(header[21:25]))
	payload := make([]byte, keyLen+valueLen)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return Entry{}, err
	}
	return Entry{
		Key: string(payload[:keyLen]), Value: append([]byte(nil), payload[keyLen:]...),
		Timestamp: int64(binary.BigEndian.Uint64(header[1:9])),
		Sequence:  binary.BigEndian.Uint64(header[9:17]), Type: header[0],
	}, nil
}

package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	fileHeaderSize   = 8
	recordHeaderSize = 25
	maxRecordBytes   = uint64(^uint32(0))

	opPut    byte = 1
	opDelete byte = 2
)

var (
	walMagic = [fileHeaderSize]byte{'M', 'K', 'V', 'W', 'A', 'L', 2, 0}
	sstMagic = [fileHeaderSize]byte{'M', 'K', 'V', 'S', 'S', 'T', 2, 0}
)

// IndexRecord 指向某个 key 当前胜出的磁盘条目。实际 Value 仍保存在 SSTable，
// 从而让内存索引保持紧凑。
type IndexRecord struct {
	SSTFile   string
	Offset    int64
	Size      int
	Sequence  uint64
	Timestamp int64
	Type      ValueType
}

type diskRecord struct {
	key       string
	value     string
	sequence  uint64
	timestamp int64
	type_     ValueType
}

// KVStore 管理 dataDir 下的全部文件。不同节点必须使用不同目录，
// 以免彼此的 WAL 和 SSTable 混在一起。
type KVStore struct {
	mu sync.RWMutex

	dataDir string
	walPath string

	memTable   map[string]InternalEntry
	memSize    int
	maxMemSize int

	lastFlushTime time.Time
	maxFlushTime  time.Duration

	walFile      *os.File
	index        map[string]IndexRecord
	nextSSTID    int
	nextSequence uint64

	stopCh    chan struct{}
	stopOnce  sync.Once
	flusherWG sync.WaitGroup
}

// NewKVStore 打开（或创建）以 dataDir 为根目录的存储。启动时先建立 SSTable
// 索引，再回放 WAL，最后才接收新写入；这样 WAL 条目可以按 LWW 覆盖磁盘条目。
func NewKVStore(dataDir string) (*KVStore, error) {
	if dataDir == "" {
		return nil, errors.New("data directory is empty")
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	_, statErr := os.Stat(absDir)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("stat data directory: %w", statErr)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	if created {
		if err := syncDir(filepath.Dir(absDir)); err != nil {
			return nil, fmt.Errorf("sync data directory parent: %w", err)
		}
	}

	kv := &KVStore{
		dataDir:       absDir,
		walPath:       filepath.Join(absDir, "wal.log"),
		memTable:      make(map[string]InternalEntry),
		maxMemSize:    1000,
		index:         make(map[string]IndexRecord),
		lastFlushTime: time.Now(),
		maxFlushTime:  60 * time.Second,
		stopCh:        make(chan struct{}),
	}

	if err := kv.loadIndexFromSSTables(); err != nil {
		return nil, err
	}
	validWALSize, err := kv.loadFromWAL()
	if err != nil {
		return nil, err
	}
	if err := kv.openWAL(validWALSize); err != nil {
		return nil, err
	}

	kv.flusherWG.Add(1)
	go kv.backgroundFlusher()
	return kv, nil
}

func (kv *KVStore) sstableFiles() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(kv.dataDir, "data_*.sst"))
	if err != nil {
		return nil, fmt.Errorf("list SSTables: %w", err)
	}
	sort.Slice(files, func(i, j int) bool { return sstableID(files[i]) < sstableID(files[j]) })
	return files, nil
}

func sstableID(path string) int {
	name := filepath.Base(path)
	id, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "data_"), ".sst"))
	if err != nil {
		return -1
	}
	return id
}

// loadIndexFromSSTables 重建点查索引，并让文件编号和本地 sequence
// 都越过所有已持久化的 SSTable 条目。
func (kv *KVStore) loadIndexFromSSTables() error {
	files, err := kv.sstableFiles()
	if err != nil {
		return err
	}
	maxID := -1
	for _, filename := range files {
		id := sstableID(filename)
		if id < 0 {
			continue
		}
		if id > maxID {
			maxID = id
		}
		if err := kv.scanSSTable(filename); err != nil {
			return err
		}
	}
	kv.nextSSTID = maxID + 1
	return nil
}

// scanSSTable 遍历一个不可变文件的全部条目，并只在 kv.index 中保留每个 key
// 的 LWW 胜出版本。
func (kv *KVStore) scanSSTable(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open SSTable %s: %w", path, err)
	}
	defer file.Close()
	if err := readFileHeader(file, sstMagic); err != nil {
		return fmt.Errorf("read SSTable %s: %w", path, err)
	}
	offset := int64(fileHeaderSize)
	for {
		record, size, err := readRecord(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read SSTable %s at offset %d: %w", path, offset, err)
		}
		old, exists := kv.index[record.key]
		// 文件按 generation 递增扫描；时间戳相同时优先新文件，
		// 这能正确处理 compaction 发布后、旧文件删除前的崩溃恢复。
		candidate := entryFromDisk(record)
		wins := !exists
		if exists {
			current, err := entryFromIndex(old)
			if err != nil {
				return fmt.Errorf("read indexed record for %q: %w", record.key, err)
			}
			wins = compareVersions(candidate, current) >= 0
		}
		if wins {
			kv.index[record.key] = IndexRecord{
				SSTFile: path, Offset: offset, Size: size,
				Sequence: record.sequence, Timestamp: record.timestamp, Type: record.type_,
			}
		}
		if record.sequence > kv.nextSequence {
			kv.nextSequence = record.sequence
		}
		offset += int64(size)
	}
	return nil
}

// loadFromWAL 只容忍末尾记录不完整，这是进程或电源故障的预期结果；
// 返回的有效前缀会在后续打开 WAL 时截断保留。
func (kv *KVStore) loadFromWAL() (int64, error) {
	file, err := os.Open(kv.walPath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open WAL: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat WAL: %w", err)
	}
	if info.Size() == 0 {
		return 0, nil
	}
	if err := readFileHeader(file, walMagic); err != nil {
		return 0, fmt.Errorf("read WAL header: %w", err)
	}
	validSize := int64(fileHeaderSize)
	for {
		record, size, err := readRecord(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("read WAL at offset %d: %w", validSize, err)
		}
		validSize += int64(size)
		entry, inMem := kv.memTable[record.key]
		indexed, onDisk := kv.index[record.key]
		candidate := entryFromDisk(record)
		if inMem && compareVersions(candidate, entry) <= 0 {
			continue
		}
		if onDisk {
			indexedEntry, err := entryFromIndex(indexed)
			if err != nil {
				return 0, fmt.Errorf("read indexed record for %q: %w", record.key, err)
			}
			if compareVersions(candidate, indexedEntry) <= 0 {
				continue
			}
		}
		if !inMem {
			kv.memSize++
		}
		kv.memTable[record.key] = candidate
		if record.sequence > kv.nextSequence {
			kv.nextSequence = record.sequence
		}
	}
	return validSize, nil
}

// openWAL 为新 WAL 写入文件头，或截断恢复阶段发现的损坏尾部。重新追加前，
// 文件头和截断结果都必须先同步到磁盘。
func (kv *KVStore) openWAL(validSize int64) error {
	file, err := os.OpenFile(kv.walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			if err := file.Close(); err != nil {
				fmt.Printf("[storage] close failed WAL handle: %v\n", err)
			}
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL: %w", err)
	}
	if validSize == 0 {
		if err := file.Truncate(0); err != nil {
			return fmt.Errorf("initialize WAL: %w", err)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek WAL: %w", err)
		}
		if err := writeAll(file, walMagic[:]); err != nil {
			return fmt.Errorf("write WAL header: %w", err)
		}
		validSize = fileHeaderSize
	} else if info.Size() != validSize {
		if err := file.Truncate(validSize); err != nil {
			return fmt.Errorf("truncate incomplete WAL tail: %w", err)
		}
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	if err := syncDir(kv.dataDir); err != nil {
		return fmt.Errorf("sync data directory: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL end: %w", err)
	}
	kv.walFile = file
	failed = false
	return nil
}

// Put 写入一个带版本的值。Timestamp 是跨节点的 LWW 版本；候选值在本地胜出后
// 才由存储层分配 sequence。
func (kv *KVStore) Put(key, value string, timestamp int64) error {
	return kv.put(key, value, timestamp, ValuePut)
}

// Delete 写入一个带版本的 tombstone。它与 Put 使用相同的 LWW 时间戳语义，
// 且永远不会编码为对用户可见的特殊值。
func (kv *KVStore) Delete(key string, timestamp int64) error {
	return kv.put(key, "", timestamp, ValueDelete)
}

// put 是唯一的写入路径。它会在追加 WAL 前拒绝 LWW 失效候选值；已接受的条目
// 必须先 fsync 到 WAL，之后才能从 MemTable 读取。
func (kv *KVStore) put(key, value string, timestamp int64, valueType ValueType) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.walFile == nil {
		return errors.New("store is closed")
	}
	candidate := InternalEntry{Key: key, Value: []byte(value), Timestamp: timestamp, Type: valueType}
	if entry, exists := kv.memTable[key]; exists && compareVersions(candidate, entry) <= 0 {
		return nil
	}
	if record, exists := kv.index[key]; exists {
		indexedEntry, err := entryFromIndex(record)
		if err != nil {
			return fmt.Errorf("read indexed record for %q: %w", key, err)
		}
		if compareVersions(candidate, indexedEntry) <= 0 {
			return nil
		}
	}
	if kv.memSize >= kv.maxMemSize {
		if err := kv.flush(); err != nil {
			return err
		}
	}

	kv.nextSequence++
	candidate.Sequence = kv.nextSequence
	record := diskFromEntry(candidate)
	if _, err := writeRecord(kv.walFile, record); err != nil {
		return fmt.Errorf("append WAL: %w", err)
	}
	if err := kv.walFile.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	if _, exists := kv.memTable[key]; !exists {
		kv.memSize++
	}
	kv.memTable[key] = candidate
	return nil
}

// Get 返回带类型的读取结果。tombstone 通过 Deleted 表示，而不是编码进 Value；
// 同时保留其 Timestamp，供副本间 LWW 比较。
func (kv *KVStore) Get(key string) ReadResult {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if entry, exists := kv.memTable[key]; exists {
		return ReadResult{Value: append([]byte(nil), entry.Value...), Timestamp: entry.Timestamp, Found: entry.Type == ValuePut, Deleted: entry.Type == ValueDelete}
	}
	record, exists := kv.index[key]
	if !exists {
		return ReadResult{}
	}
	entry, err := readRecordAt(record)
	if err != nil {
		return ReadResult{}
	}
	return ReadResult{Value: []byte(entry.value), Timestamp: entry.timestamp, Found: entry.type_ == ValuePut, Deleted: entry.type_ == ValueDelete}
}

// readRecordAt 按索引精确读取一条记录，无需扫描整个 SSTable。
func readRecordAt(index IndexRecord) (diskRecord, error) {
	file, err := os.Open(index.SSTFile)
	if err != nil {
		return diskRecord{}, err
	}
	defer file.Close()
	section := io.NewSectionReader(file, index.Offset, int64(index.Size))
	record, _, err := readRecord(section)
	return record, err
}

// entryFromIndex 只在时间戳相同、需要比较 Value 的冲突场景中，才将索引条目
// 实体化为完整的 InternalEntry。
func entryFromIndex(index IndexRecord) (InternalEntry, error) {
	record, err := readRecordAt(index)
	if err != nil {
		return InternalEntry{}, err
	}
	return entryFromDisk(record), nil
}

// flush 将当前 MemTable 原子发布为新的 SSTable。发布顺序是：写入并 fsync
// 临时文件、rename 并 fsync 目录、重置 WAL。任一步骤间崩溃都至多回放已落盘条目。
func (kv *KVStore) flush() error {
	if kv.memSize == 0 {
		return nil
	}
	finalPath := filepath.Join(kv.dataDir, fmt.Sprintf("data_%d.sst", kv.nextSSTID))
	temp, err := os.CreateTemp(kv.dataDir, ".flush-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary SSTable: %w", err)
	}
	tempPath := temp.Name()
	committed := false
	defer func() {
		if !committed {
			if err := temp.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				fmt.Printf("[storage] close failed flush file: %v\n", err)
			}
			if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				fmt.Printf("[storage] remove failed flush file: %v\n", err)
			}
		}
	}()

	if err := writeAll(temp, sstMagic[:]); err != nil {
		return fmt.Errorf("write SSTable header: %w", err)
	}
	keys := make([]string, 0, len(kv.memTable))
	for key := range kv.memTable {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	offset := int64(fileHeaderSize)
	newRecords := make(map[string]IndexRecord, len(keys))
	for _, key := range keys {
		entry := kv.memTable[key]
		size, err := writeRecord(temp, diskRecord{
			key: key, value: string(entry.Value), sequence: entry.Sequence, timestamp: entry.Timestamp, type_: entry.Type,
		})
		if err != nil {
			return fmt.Errorf("write SSTable record: %w", err)
		}
		newRecords[key] = IndexRecord{
			SSTFile: finalPath, Offset: offset, Size: size,
			Sequence: entry.Sequence, Timestamp: entry.Timestamp, Type: entry.Type,
		}
		offset += int64(size)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync temporary SSTable: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary SSTable: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("publish SSTable: %w", err)
	}
	if err := syncDir(kv.dataDir); err != nil {
		return fmt.Errorf("sync SSTable directory: %w", err)
	}
	committed = true

	// SSTable 先于 WAL 重置持久化；两者之间崩溃只会回放重复条目，
	// LWW 比较会安全地忽略它们。
	if err := kv.resetWAL(); err != nil {
		return err
	}
	for key, record := range newRecords {
		kv.index[key] = record
	}
	kv.nextSSTID++
	kv.memTable = make(map[string]InternalEntry)
	kv.memSize = 0
	kv.lastFlushTime = time.Now()
	return nil
}

// resetWAL 只能在对应的 SSTable 已经持久化后调用。
func (kv *KVStore) resetWAL() error {
	if err := kv.walFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}
	if _, err := kv.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	if err := writeAll(kv.walFile, walMagic[:]); err != nil {
		return fmt.Errorf("rewrite WAL header: %w", err)
	}
	if err := kv.walFile.Sync(); err != nil {
		return fmt.Errorf("sync reset WAL: %w", err)
	}
	return nil
}

// backgroundFlusher 定期 flush 超时的 MemTable，并在 SSTable 数量达到当前
// 原型阈值时执行 compaction。
func (kv *KVStore) backgroundFlusher() {
	defer kv.flusherWG.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			kv.mu.Lock()
			if kv.memSize > 0 && time.Since(kv.lastFlushTime) >= kv.maxFlushTime {
				if err := kv.flush(); err != nil {
					fmt.Printf("[storage] background flush failed: %v\n", err)
				}
			}
			files, err := kv.sstableFiles()
			if err != nil {
				fmt.Printf("[storage] list SSTables failed: %v\n", err)
			} else if len(files) >= 5 {
				if err := kv.compact(); err != nil {
					fmt.Printf("[storage] background compaction failed: %v\n", err)
				}
			}
			kv.mu.Unlock()
		case <-kv.stopCh:
			return
		}
	}
}

// Compact 将每个 key 的最新版本写成新 SSTable 中唯一的条目。它与写入串行执行，
// 以保持索引和文件的一致性。
func (kv *KVStore) Compact() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.walFile == nil {
		return errors.New("store is closed")
	}
	return kv.compact()
}

// compact 是显式和后台 compaction 共用的、已持锁实现。它先 flush 易失数据，
// 使后续合并只读取 SSTable。
func (kv *KVStore) compact() error {
	if kv.memSize > 0 {
		if err := kv.flush(); err != nil {
			return err
		}
	}
	oldFiles, err := kv.sstableFiles()
	if err != nil {
		return err
	}
	if len(oldFiles) <= 1 {
		return nil
	}

	keys := make([]string, 0, len(kv.index))
	for key := range kv.index {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	records := make([]diskRecord, 0, len(keys))
	for _, key := range keys {
		record, err := readRecordAt(kv.index[key])
		if err != nil {
			return fmt.Errorf("read current value for %q: %w", key, err)
		}
		records = append(records, record)
	}

	finalPath := filepath.Join(kv.dataDir, fmt.Sprintf("data_%d.sst", kv.nextSSTID))
	temp, err := os.CreateTemp(kv.dataDir, ".compact-*.tmp")
	if err != nil {
		return fmt.Errorf("create compaction file: %w", err)
	}
	tempPath := temp.Name()
	committed := false
	defer func() {
		if !committed {
			if err := temp.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				fmt.Printf("[storage] close failed compaction file: %v\n", err)
			}
			if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				fmt.Printf("[storage] remove failed compaction file: %v\n", err)
			}
		}
	}()
	if err := writeAll(temp, sstMagic[:]); err != nil {
		return fmt.Errorf("write compacted SSTable header: %w", err)
	}
	newIndex := make(map[string]IndexRecord, len(records))
	offset := int64(fileHeaderSize)
	for _, record := range records {
		size, err := writeRecord(temp, record)
		if err != nil {
			return fmt.Errorf("write compacted SSTable record: %w", err)
		}
		newIndex[record.key] = IndexRecord{
			SSTFile: finalPath, Offset: offset, Size: size,
			Sequence: record.sequence, Timestamp: record.timestamp, Type: record.type_,
		}
		offset += int64(size)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync compacted SSTable: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close compacted SSTable: %w", err)
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("publish compacted SSTable: %w", err)
	}
	if err := syncDir(kv.dataDir); err != nil {
		return fmt.Errorf("sync compacted SSTable directory: %w", err)
	}
	committed = true

	// 仅在 rename+fsync 后发布新的内存视图；旧文件随后才删除，
	// 因而任意崩溃点都至少保留一份完整数据。
	kv.index = newIndex
	kv.nextSSTID++
	for _, path := range oldFiles {
		if path == finalPath {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove old SSTable %s: %w", path, err)
		}
	}
	if err := syncDir(kv.dataDir); err != nil {
		return fmt.Errorf("sync SSTable removals: %w", err)
	}
	return nil
}

// Close 停止后台任务并执行一次持久化 flush。
func (kv *KVStore) Close() error {
	kv.stopOnce.Do(func() { close(kv.stopCh) })
	kv.flusherWG.Wait()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.walFile == nil {
		return nil
	}
	var result error
	if err := kv.flush(); err != nil {
		result = err
	}
	if err := kv.walFile.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close WAL: %w", err))
	}
	kv.walFile = nil
	return result
}

// writeRecord 使用固定的 25 字节大端序头部：操作类型(1)、timestamp(8)、
// sequence(8)、key 长度(4)、value 长度(4)。
func writeRecord(w io.Writer, record diskRecord) (int, error) {
	keyLen, valueLen := uint64(len(record.key)), uint64(len(record.value))
	if keyLen > maxRecordBytes || valueLen > maxRecordBytes {
		return 0, errors.New("key or value exceeds uint32 encoding limit")
	}
	header := make([]byte, recordHeaderSize)
	if record.type_ == ValueDelete {
		header[0] = opDelete
	} else {
		header[0] = opPut
	}
	binary.BigEndian.PutUint64(header[1:9], uint64(record.timestamp))
	binary.BigEndian.PutUint64(header[9:17], record.sequence)
	binary.BigEndian.PutUint32(header[17:21], uint32(keyLen))
	binary.BigEndian.PutUint32(header[21:25], uint32(valueLen))
	if err := writeAll(w, header); err != nil {
		return 0, err
	}
	if err := writeAll(w, []byte(record.key)); err != nil {
		return 0, err
	}
	if err := writeAll(w, []byte(record.value)); err != nil {
		return 0, err
	}
	return recordHeaderSize + len(record.key) + len(record.value), nil
}

// readRecord 校验已编码记录。若头部或载荷被截断则返回 io.ErrUnexpectedEOF，
// 使 WAL 恢复只丢弃损坏尾部。
func readRecord(r io.Reader) (diskRecord, int, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return diskRecord{}, 0, err
	}
	if header[0] != opPut && header[0] != opDelete {
		return diskRecord{}, 0, fmt.Errorf("unknown operation %d", header[0])
	}
	keyLen := uint64(binary.BigEndian.Uint32(header[17:21]))
	valueLen := uint64(binary.BigEndian.Uint32(header[21:25]))
	total := uint64(recordHeaderSize) + keyLen + valueLen
	if total > uint64(int(^uint(0)>>1)) {
		return diskRecord{}, 0, errors.New("record is too large for this platform")
	}
	payload := make([]byte, int(keyLen+valueLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return diskRecord{}, 0, err
	}
	return diskRecord{
		key: string(payload[:keyLen]), value: string(payload[keyLen:]),
		sequence: binary.BigEndian.Uint64(header[9:17]), timestamp: int64(binary.BigEndian.Uint64(header[1:9])), type_: ValueType(header[0]),
	}, int(total), nil
}

// entryFromDisk 和 diskFromEntry 隔离磁盘编码与带类型的内部版本模型。
func entryFromDisk(record diskRecord) InternalEntry {
	return InternalEntry{Key: record.key, Value: []byte(record.value), Sequence: record.sequence, Timestamp: record.timestamp, Type: record.type_}
}

func diskFromEntry(entry InternalEntry) diskRecord {
	return diskRecord{key: entry.Key, value: string(entry.Value), sequence: entry.Sequence, timestamp: entry.Timestamp, type_: entry.Type}
}

// readFileHeader 会在解码变长记录前拒绝其他格式版本的文件。
func readFileHeader(r io.Reader, expected [fileHeaderSize]byte) error {
	header := make([]byte, fileHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if string(header) != string(expected[:]) {
		return errors.New("unsupported or corrupt file format")
	}
	return nil
}

// writeAll 处理可能分多次完成写入的 Writer。
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

// syncDir 在仅 fsync 文件不足以保证目录元数据持久化的文件系统上，
// 让 rename 或删除操作真正落盘。
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		if closeErr := dir.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}
	return dir.Close()
}

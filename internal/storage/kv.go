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
	recordHeaderSize = 17
	maxRecordBytes   = uint64(^uint32(0))

	opPut    byte = 1
	opDelete byte = 2
)

var (
	walMagic = [fileHeaderSize]byte{'M', 'K', 'V', 'W', 'A', 'L', 1, 0}
	sstMagic = [fileHeaderSize]byte{'M', 'K', 'V', 'S', 'S', 'T', 1, 0}
)

// ValueEntry stores a value and its timestamp for LWW conflict resolution.
type ValueEntry struct {
	Value     string
	Timestamp int64
	Deleted   bool
}

type IndexRecord struct {
	SSTFile   string
	Offset    int64
	Size      int
	Timestamp int64
	Deleted   bool
}

type diskRecord struct {
	key       string
	value     string
	timestamp int64
	deleted   bool
}

// KVStore owns every file below dataDir. Different nodes must use different
// directories; this prevents their WALs and SSTables from being mixed.
type KVStore struct {
	mu sync.RWMutex

	dataDir string
	walPath string

	memTable   map[string]ValueEntry
	memSize    int
	maxMemSize int

	lastFlushTime time.Time
	maxFlushTime  time.Duration

	walFile   *os.File
	index     map[string]IndexRecord
	nextSSTID int

	stopCh    chan struct{}
	stopOnce  sync.Once
	flusherWG sync.WaitGroup
}

// NewKVStore opens (or creates) a store rooted at dataDir.
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
		memTable:      make(map[string]ValueEntry),
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
		// Files are scanned by increasing generation. On an equal timestamp,
		// prefer the newer generation (important after a compaction crash).
		if !exists || record.timestamp >= old.Timestamp {
			kv.index[record.key] = IndexRecord{
				SSTFile: path, Offset: offset, Size: size,
				Timestamp: record.timestamp, Deleted: record.deleted,
			}
		}
		offset += int64(size)
	}
	return nil
}

// loadFromWAL tolerates only an incomplete final record. This is the expected
// result of a process/power failure; the valid prefix is returned for truncation.
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
		if (inMem && record.timestamp <= entry.Timestamp) || (onDisk && record.timestamp <= indexed.Timestamp) {
			continue
		}
		if !inMem {
			kv.memSize++
		}
		kv.memTable[record.key] = ValueEntry{
			Value: record.value, Timestamp: record.timestamp, Deleted: record.deleted,
		}
	}
	return validSize, nil
}

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

func (kv *KVStore) Put(key, value string, timestamp int64) error {
	return kv.put(key, value, timestamp, false)
}

func (kv *KVStore) Delete(key string, timestamp int64) error {
	return kv.put(key, "", timestamp, true)
}

func (kv *KVStore) put(key, value string, timestamp int64, deleted bool) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.walFile == nil {
		return errors.New("store is closed")
	}
	if entry, exists := kv.memTable[key]; exists && timestamp <= entry.Timestamp {
		return nil
	}
	if record, exists := kv.index[key]; exists && timestamp <= record.Timestamp {
		return nil
	}
	if kv.memSize >= kv.maxMemSize {
		if err := kv.flush(); err != nil {
			return err
		}
	}

	record := diskRecord{key: key, value: value, timestamp: timestamp, deleted: deleted}
	if _, err := writeRecord(kv.walFile, record); err != nil {
		return fmt.Errorf("append WAL: %w", err)
	}
	if err := kv.walFile.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	if _, exists := kv.memTable[key]; !exists {
		kv.memSize++
	}
	kv.memTable[key] = ValueEntry{Value: value, Timestamp: timestamp, Deleted: deleted}
	return nil
}

// Get returns the legacy tombstone marker for deletes so existing replication
// code can continue to perform LWW/read-repair without changing its API.
func (kv *KVStore) Get(key string) (string, int64, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if entry, exists := kv.memTable[key]; exists {
		if entry.Deleted {
			return "<TOMBSTONE>", entry.Timestamp, true
		}
		return entry.Value, entry.Timestamp, true
	}
	record, exists := kv.index[key]
	if !exists {
		return "", 0, false
	}
	entry, err := readRecordAt(record)
	if err != nil {
		return "", 0, false
	}
	if entry.deleted {
		return "<TOMBSTONE>", entry.timestamp, true
	}
	return entry.value, entry.timestamp, true
}

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
			key: key, value: entry.Value, timestamp: entry.Timestamp, deleted: entry.Deleted,
		})
		if err != nil {
			return fmt.Errorf("write SSTable record: %w", err)
		}
		newRecords[key] = IndexRecord{
			SSTFile: finalPath, Offset: offset, Size: size,
			Timestamp: entry.Timestamp, Deleted: entry.Deleted,
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

	// The SSTable is durable before the WAL is reset. A crash between these
	// steps merely replays duplicates, which LWW safely ignores.
	if err := kv.resetWAL(); err != nil {
		return err
	}
	for key, record := range newRecords {
		kv.index[key] = record
	}
	kv.nextSSTID++
	kv.memTable = make(map[string]ValueEntry)
	kv.memSize = 0
	kv.lastFlushTime = time.Now()
	return nil
}

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

func (kv *KVStore) Compact() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.walFile == nil {
		return errors.New("store is closed")
	}
	return kv.compact()
}

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
			Timestamp: record.timestamp, Deleted: record.deleted,
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

	// Publish the new in-memory view only after rename+fsync. Old files are
	// removed afterwards, so every crash point leaves at least one full copy.
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

// Close performs a durable flush and stops the background worker.
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

func writeRecord(w io.Writer, record diskRecord) (int, error) {
	keyLen, valueLen := uint64(len(record.key)), uint64(len(record.value))
	if keyLen > maxRecordBytes || valueLen > maxRecordBytes {
		return 0, errors.New("key or value exceeds uint32 encoding limit")
	}
	header := make([]byte, recordHeaderSize)
	if record.deleted {
		header[0] = opDelete
	} else {
		header[0] = opPut
	}
	binary.BigEndian.PutUint64(header[1:9], uint64(record.timestamp))
	binary.BigEndian.PutUint32(header[9:13], uint32(keyLen))
	binary.BigEndian.PutUint32(header[13:17], uint32(valueLen))
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

func readRecord(r io.Reader) (diskRecord, int, error) {
	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return diskRecord{}, 0, err
	}
	if header[0] != opPut && header[0] != opDelete {
		return diskRecord{}, 0, fmt.Errorf("unknown operation %d", header[0])
	}
	keyLen := uint64(binary.BigEndian.Uint32(header[9:13]))
	valueLen := uint64(binary.BigEndian.Uint32(header[13:17]))
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
		timestamp: int64(binary.BigEndian.Uint64(header[1:9])), deleted: header[0] == opDelete,
	}, int(total), nil
}

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

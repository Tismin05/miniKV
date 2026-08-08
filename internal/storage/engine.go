package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"miniKV/internal/storage/compaction"
	"miniKV/internal/storage/sstable"
	"miniKV/internal/storage/wal"
)

const (
	defaultMaxMemTableEntries = 1000
	defaultMaxMemTableBytes   = 4 << 20
	defaultMaxImmutables      = 2
)

// Options 控制 MemTable rotation、Flush 重试和 block-based SSTable。
type Options struct {
	MaxMemTableEntries int
	MaxMemTableBytes   int
	MaxImmutableTables int
	FlushInterval      time.Duration
	FlushRetryInterval time.Duration
	SSTable            sstable.Options
	BlockCache         sstable.BlockCache
}

func (o Options) normalized() Options {
	if o.MaxMemTableEntries <= 0 {
		o.MaxMemTableEntries = defaultMaxMemTableEntries
	}
	if o.MaxMemTableBytes <= 0 {
		o.MaxMemTableBytes = defaultMaxMemTableBytes
	}
	if o.MaxImmutableTables <= 0 {
		o.MaxImmutableTables = defaultMaxImmutables
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 60 * time.Second
	}
	if o.FlushRetryInterval <= 0 {
		o.FlushRetryInterval = 100 * time.Millisecond
	}
	return o
}

type immutableMemTable struct {
	table   *mapMemTable
	walPath string
}

type tableHandle struct {
	generation int
	reader     *sstable.Reader
}

// Engine 只负责 API、锁顺序和组件编排；文件格式及生命周期分别由 WAL/SSTable 管理。
type Engine struct {
	mu   sync.RWMutex
	cond *sync.Cond

	dataDir string
	options Options

	active       *mapMemTable
	immutables   []*immutableMemTable
	tables       []*tableHandle
	nextSSTID    int
	nextSequence uint64

	wal *wal.WAL

	// maxMemSize 保留包内测试与旧实现调参习惯；新调用方应使用 Options。
	maxMemSize   int
	maxFlushTime time.Duration
	lastRotation time.Time

	flushNotify  chan struct{}
	stopCh       chan struct{}
	stopOnce     sync.Once
	workers      sync.WaitGroup
	closing      bool
	closed       bool
	lastFlushErr error

	writeTable func(string, []sstable.Entry) (sstable.Properties, error)
}

// KVStore 是旧名称的兼容别名，现有 Put/Get/Delete/Close 调用无需迁移。
type KVStore = Engine

// NewKVStore 使用默认参数创建 Engine。
func NewKVStore(dataDir string) (*Engine, error) {
	return NewEngine(dataDir, Options{})
}

// NewEngine 先打开 SSTable，再重建 Immutable/Active MemTable，最后启动后台 Flush。
func NewEngine(dataDir string, options Options) (*Engine, error) {
	if dataDir == "" {
		return nil, errors.New("data directory is empty")
	}
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	created, err := ensureDataDir(absDir)
	if err != nil {
		return nil, err
	}
	if created {
		if err := syncDir(filepath.Dir(absDir)); err != nil {
			return nil, fmt.Errorf("sync data directory parent: %w", err)
		}
	}

	options = options.normalized()
	engine := &Engine{
		dataDir:      absDir,
		options:      options,
		active:       newMemTable(),
		maxMemSize:   options.MaxMemTableEntries,
		maxFlushTime: options.FlushInterval,
		lastRotation: time.Now(),
		flushNotify:  make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
	}
	engine.cond = sync.NewCond(&engine.mu)
	writer := sstable.NewWriter(options.SSTable)
	engine.writeTable = writer.Write

	if err := engine.openTables(); err != nil {
		engine.closeTables()
		return nil, err
	}
	log, recovery, err := wal.Open(absDir)
	if err != nil {
		engine.closeTables()
		return nil, err
	}
	engine.wal = log
	engine.recoverMemTables(recovery)

	engine.workers.Add(2)
	go engine.flushWorker()
	go engine.maintenanceWorker()
	if len(engine.immutables) > 0 {
		engine.notifyFlush()
	}
	return engine, nil
}

func ensureDataDir(path string) (bool, error) {
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return false, fmt.Errorf("stat data directory: %w", statErr)
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return false, fmt.Errorf("create data directory: %w", err)
	}
	return created, nil
}

func (e *Engine) openTables() error {
	descriptors, err := sstable.OpenAll(e.dataDir, e.options.BlockCache)
	if err != nil {
		return err
	}
	maxID := -1
	for _, descriptor := range descriptors {
		e.tables = append(e.tables, &tableHandle{generation: descriptor.Generation, reader: descriptor.Reader})
		if sequence := descriptor.Reader.Properties().MaxSequence; sequence > e.nextSequence {
			e.nextSequence = sequence
		}
		if descriptor.Generation > maxID {
			maxID = descriptor.Generation
		}
	}
	e.nextSSTID = maxID + 1
	return nil
}

func (e *Engine) recoverMemTables(recovery wal.Recovery) {
	for _, segment := range recovery.Sealed {
		table := newMemTable()
		for _, record := range segment.Records {
			entry := entryFromWAL(record)
			table.Set(entry)
			if entry.Sequence > e.nextSequence {
				e.nextSequence = entry.Sequence
			}
		}
		if table.Len() == 0 {
			_ = e.wal.RemoveSegment(segment.Path)
			continue
		}
		e.immutables = append(e.immutables, &immutableMemTable{table: table, walPath: segment.Path})
	}
	for _, record := range recovery.Active {
		entry := entryFromWAL(record)
		e.active.Set(entry)
		if entry.Sequence > e.nextSequence {
			e.nextSequence = entry.Sequence
		}
	}
}

// Put 写入带跨节点 LWW 时间戳的值。
func (e *Engine) Put(key, value string, timestamp int64) error {
	return e.put(key, []byte(value), timestamp, ValuePut)
}

// Delete 写入显式 tombstone，不占用用户值编码空间。
func (e *Engine) Delete(key string, timestamp int64) error {
	return e.put(key, nil, timestamp, ValueDelete)
}

func (e *Engine) put(key string, value []byte, timestamp int64, valueType ValueType) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing || e.closed || e.wal == nil {
		return errors.New("store is closed")
	}
	candidate := InternalEntry{Key: key, Value: append([]byte(nil), value...), Timestamp: timestamp, Type: valueType}
	current, exists, err := e.getEntryLocked(key)
	if err != nil {
		return fmt.Errorf("read current version for %q: %w", key, err)
	}
	if exists && compareVersions(candidate, current) <= 0 {
		return nil
	}

	// 当 Active 已达阈值且 Immutable 队列已满时，cond.Wait 会释放写锁，
	// Flush Worker 仍可发布文件；这就是明确的写入背压点。
	for e.shouldRotateLocked() && len(e.immutables) >= e.options.MaxImmutableTables {
		e.cond.Wait()
		if e.closing || e.closed {
			return errors.New("store is closed")
		}
	}
	if e.shouldRotateLocked() {
		if err := e.rotateLocked(); err != nil {
			return err
		}
	}

	e.nextSequence++
	candidate.Sequence = e.nextSequence
	if err := e.wal.Append(walFromEntry(candidate)); err != nil {
		e.nextSequence--
		return err
	}
	e.active.Set(candidate)
	if e.shouldRotateLocked() && len(e.immutables) < e.options.MaxImmutableTables {
		if err := e.rotateLocked(); err != nil {
			// 记录已同步且仍在 Active 中，调用方可安全重试或重启恢复。
			return fmt.Errorf("rotate durable MemTable: %w", err)
		}
	}
	return nil
}

func (e *Engine) shouldRotateLocked() bool {
	return e.active.Len() > 0 && (e.active.Len() >= e.maxMemSize || e.active.SizeBytes() >= e.options.MaxMemTableBytes)
}

// rotateLocked 只交换指针、封存 WAL 和入队，不执行 SSTable I/O。
func (e *Engine) rotateLocked() error {
	if e.active.Len() == 0 {
		return nil
	}
	segmentPath, err := e.wal.Rotate()
	if err != nil {
		return fmt.Errorf("rotate WAL: %w", err)
	}
	e.immutables = append(e.immutables, &immutableMemTable{table: e.active, walPath: segmentPath})
	e.active = newMemTable()
	e.lastRotation = time.Now()
	e.notifyFlush()
	return nil
}

func (e *Engine) notifyFlush() {
	select {
	case e.flushNotify <- struct{}{}:
	default:
	}
}

// Get 保持旧 API；需要感知磁盘损坏的调用方应使用 GetWithError。
func (e *Engine) Get(key string) ReadResult {
	result, _ := e.GetWithError(key)
	return result
}

// GetWithError 在块 checksum 失败时向上返回明确错误。
func (e *Engine) GetWithError(key string) (ReadResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	entry, exists, err := e.getEntryLocked(key)
	if err != nil {
		return ReadResult{}, err
	}
	if !exists {
		return ReadResult{}, nil
	}
	return ReadResult{
		Value: append([]byte(nil), entry.Value...), Timestamp: entry.Timestamp,
		Found: entry.Type == ValuePut, Deleted: entry.Type == ValueDelete,
	}, nil
}

func (e *Engine) getEntryLocked(key string) (InternalEntry, bool, error) {
	var winner InternalEntry
	found := false
	consider := func(candidate InternalEntry) {
		if !found || compareVersions(candidate, winner) > 0 {
			winner = candidate
			found = true
		}
	}
	if entry, ok := e.active.Get(key); ok {
		consider(entry)
	}
	for i := len(e.immutables) - 1; i >= 0; i-- {
		if entry, ok := e.immutables[i].table.Get(key); ok {
			consider(entry)
		}
	}
	for i := len(e.tables) - 1; i >= 0; i-- {
		entry, ok, err := e.tables[i].reader.Get(key)
		if err != nil {
			return InternalEntry{}, false, err
		}
		if ok {
			consider(entryFromSSTable(entry))
		}
	}
	return winner, found, nil
}

func (e *Engine) flushWorker() {
	defer e.workers.Done()
	for {
		select {
		case <-e.flushNotify:
		case <-e.stopCh:
			return
		}
		for {
			e.mu.RLock()
			if len(e.immutables) == 0 {
				e.mu.RUnlock()
				break
			}
			target := e.immutables[0]
			generation := e.nextSSTID
			e.mu.RUnlock()

			if err := e.flushImmutable(target, generation); err != nil {
				e.mu.Lock()
				e.lastFlushErr = err
				e.mu.Unlock()
				select {
				case <-time.After(e.options.FlushRetryInterval):
					e.notifyFlush()
				case <-e.stopCh:
					return
				}
				break
			}
		}
	}
}

func (e *Engine) flushImmutable(target *immutableMemTable, generation int) error {
	path := sstable.FilePath(e.dataDir, generation)
	entries := make([]sstable.Entry, 0, target.table.Len())
	for _, entry := range target.table.Entries() {
		entries = append(entries, sstableFromEntry(entry))
	}
	if _, err := e.writeTable(path, entries); err != nil {
		return fmt.Errorf("flush immutable MemTable: %w", err)
	}
	reader, err := sstable.Open(path, e.options.BlockCache)
	if err != nil {
		return fmt.Errorf("verify flushed SSTable: %w", err)
	}

	e.mu.Lock()
	if len(e.immutables) == 0 || e.immutables[0] != target || e.nextSSTID != generation {
		e.mu.Unlock()
		_ = reader.Close()
		return errors.New("flush publication order changed")
	}
	e.tables = append(e.tables, &tableHandle{generation: generation, reader: reader})
	e.nextSSTID++
	e.immutables = e.immutables[1:]
	e.lastFlushErr = nil
	e.cond.Broadcast()
	e.mu.Unlock()

	// 只有 v3 文件已经 rename+目录 fsync 且进入读视图后，才回收对应 WAL。
	if err := e.wal.RemoveSegment(target.walPath); err != nil {
		return fmt.Errorf("reclaim flushed WAL: %w", err)
	}
	return nil
}

func (e *Engine) maintenanceWorker() {
	defer e.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			if !e.closing && !e.closed && e.active.Len() > 0 && time.Since(e.lastRotation) >= e.maxFlushTime && len(e.immutables) < e.options.MaxImmutableTables {
				if err := e.rotateLocked(); err != nil {
					e.lastFlushErr = err
				}
			}
			e.mu.Unlock()
		case <-e.stopCh:
			return
		}
	}
}

// Flush 冻结当前 Active，并等待所有 Immutable 安全发布。
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.wal == nil {
		return errors.New("store is closed")
	}
	for e.active.Len() > 0 && len(e.immutables) >= e.options.MaxImmutableTables {
		e.cond.Wait()
	}
	if e.active.Len() > 0 {
		if err := e.rotateLocked(); err != nil {
			return err
		}
	}
	for len(e.immutables) > 0 {
		e.cond.Wait()
	}
	return e.lastFlushErr
}

// Compact 使用独立 Planner 选择输入；M4 仍保持全量合并，分层策略留给 M6。
func (e *Engine) Compact() error {
	if err := e.Flush(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closing || e.closed {
		return errors.New("store is closed")
	}
	files := make([]compaction.File, len(e.tables))
	byPath := make(map[string]*tableHandle, len(e.tables))
	for i, table := range e.tables {
		files[i] = compaction.File{Path: table.reader.Path(), Generation: table.generation}
		byPath[table.reader.Path()] = table
	}
	plan := compaction.SelectAll(files)
	if len(plan.Inputs) == 0 {
		return nil
	}

	sources := make([]compaction.Source, 0, len(plan.Inputs))
	for _, input := range plan.Inputs {
		sources = append(sources, byPath[input.Path].reader)
	}
	entries, err := compaction.Merge(sources)
	if err != nil {
		return err
	}
	path := sstable.FilePath(e.dataDir, e.nextSSTID)
	if _, err := e.writeTable(path, entries); err != nil {
		return fmt.Errorf("write compaction output: %w", err)
	}
	reader, err := sstable.Open(path, e.options.BlockCache)
	if err != nil {
		return fmt.Errorf("verify compaction output: %w", err)
	}
	newTable := &tableHandle{generation: e.nextSSTID, reader: reader}
	e.nextSSTID++
	e.tables = []*tableHandle{newTable}

	var removalErr error
	oldPaths := make([]string, 0, len(plan.Inputs))
	for _, input := range plan.Inputs {
		table := byPath[input.Path]
		if err := table.reader.Close(); err != nil {
			removalErr = errors.Join(removalErr, fmt.Errorf("close compaction input: %w", err))
		}
		oldPaths = append(oldPaths, input.Path)
	}
	removalErr = errors.Join(removalErr, sstable.RemoveFiles(e.dataDir, oldPaths))
	return removalErr
}

// Close 阻止新写入，排空所有 Immutable，再停止 worker 和关闭文件句柄。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closing = true
	for e.active.Len() > 0 && len(e.immutables) >= e.options.MaxImmutableTables {
		e.cond.Wait()
	}
	if e.active.Len() > 0 {
		if err := e.rotateLocked(); err != nil {
			e.closing = false
			e.mu.Unlock()
			return err
		}
	}
	for len(e.immutables) > 0 {
		e.cond.Wait()
	}
	e.stopOnce.Do(func() { close(e.stopCh) })
	e.mu.Unlock()
	e.workers.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	var result error
	if e.wal != nil {
		result = errors.Join(result, e.wal.Close())
		e.wal = nil
	}
	for _, table := range e.tables {
		result = errors.Join(result, table.reader.Close())
	}
	e.tables = nil
	e.closed = true
	return result
}

// Stats 暴露测试与观测所需的队列状态，不泄漏内部 map 或文件句柄。
type Stats struct {
	ActiveEntries   int
	ImmutableTables int
	SSTables        int
	LastFlushError  string
}

func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := Stats{ActiveEntries: e.active.Len(), ImmutableTables: len(e.immutables), SSTables: len(e.tables)}
	if e.lastFlushErr != nil {
		stats.LastFlushError = e.lastFlushErr.Error()
	}
	return stats
}

func (e *Engine) closeTables() {
	for _, table := range e.tables {
		_ = table.reader.Close()
	}
}

func entryFromWAL(record wal.Record) InternalEntry {
	return InternalEntry{Key: record.Key, Value: append([]byte(nil), record.Value...), Sequence: record.Sequence, Timestamp: record.Timestamp, Type: ValueType(record.Type)}
}

func walFromEntry(entry InternalEntry) wal.Record {
	return wal.Record{Key: entry.Key, Value: append([]byte(nil), entry.Value...), Sequence: entry.Sequence, Timestamp: entry.Timestamp, Type: byte(entry.Type)}
}

func entryFromSSTable(entry sstable.Entry) InternalEntry {
	return InternalEntry{Key: entry.Key, Value: append([]byte(nil), entry.Value...), Sequence: entry.Sequence, Timestamp: entry.Timestamp, Type: ValueType(entry.Type)}
}

func sstableFromEntry(entry InternalEntry) sstable.Entry {
	return sstable.Entry{Key: entry.Key, Value: append([]byte(nil), entry.Value...), Sequence: entry.Sequence, Timestamp: entry.Timestamp, Type: byte(entry.Type)}
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

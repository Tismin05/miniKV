package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const activeName = "wal.log"

// Segment 是恢复得到的已封存 WAL 及其中记录。Engine 在对应 MemTable 落盘后
// 才能调用 RemoveSegment 回收它。
type Segment struct {
	Path    string
	Records []Record
}

// Recovery 将封存段与当前活动段分开，使 Engine 能重建 Immutable/Active MemTable。
type Recovery struct {
	Sealed []Segment
	Active []Record
}

// WAL 管理活动日志及已封存 segment 的生命周期。
type WAL struct {
	dir           string
	activePath    string
	active        *os.File
	nextSegmentID uint64
}

// Open 恢复所有 segment、截断不完整尾部，并打开新的活动追加位置。
func Open(dir string) (*WAL, Recovery, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "wal_*.log"))
	if err != nil {
		return nil, Recovery{}, fmt.Errorf("list WAL segments: %w", err)
	}
	sort.Slice(paths, func(i, j int) bool { return segmentID(paths[i]) < segmentID(paths[j]) })

	recovery := Recovery{Sealed: make([]Segment, 0, len(paths))}
	var maxID uint64
	for _, path := range paths {
		records, _, err := recoverFile(path, false)
		if err != nil {
			return nil, Recovery{}, err
		}
		recovery.Sealed = append(recovery.Sealed, Segment{Path: path, Records: records})
		if id := segmentID(path); id >= maxID {
			maxID = id + 1
		}
	}

	activePath := filepath.Join(dir, activeName)
	activeRecords, validSize, err := recoverFile(activePath, true)
	if err != nil {
		return nil, Recovery{}, err
	}
	recovery.Active = activeRecords
	active, err := openActive(activePath, validSize)
	if err != nil {
		return nil, Recovery{}, err
	}
	w := &WAL{dir: dir, activePath: activePath, active: active, nextSegmentID: maxID}
	return w, recovery, nil
}

// Append 在返回前同步记录，保证随后写入 MemTable 的数据可崩溃恢复。
func (w *WAL) Append(record Record) error {
	if w.active == nil {
		return errors.New("WAL is closed")
	}
	if _, err := writeRecord(w.active, record); err != nil {
		return fmt.Errorf("append WAL: %w", err)
	}
	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	return nil
}

// Rotate 将当前活动日志原子封存，并创建新的活动日志。返回路径与刚冻结的
// Immutable MemTable 一一对应。
func (w *WAL) Rotate() (string, error) {
	if w.active == nil {
		return "", errors.New("WAL is closed")
	}
	if err := w.active.Sync(); err != nil {
		return "", fmt.Errorf("sync WAL before rotation: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return "", fmt.Errorf("close WAL before rotation: %w", err)
	}
	w.active = nil

	sealed := filepath.Join(w.dir, fmt.Sprintf("wal_%020d.log", w.nextSegmentID))
	w.nextSegmentID++
	if err := os.Rename(w.activePath, sealed); err != nil {
		return "", fmt.Errorf("seal WAL: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return "", fmt.Errorf("sync sealed WAL: %w", err)
	}
	active, err := openActive(w.activePath, 0)
	if err != nil {
		return "", err
	}
	w.active = active
	return sealed, nil
}

// RemoveSegment 只应在对应 SSTable 已原子发布后调用。
func (w *WAL) RemoveSegment(path string) error {
	if filepath.Dir(path) != w.dir || filepath.Base(path) == activeName {
		return fmt.Errorf("refuse to remove non-sealed WAL %q", path)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove WAL segment: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("sync WAL removal: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	if w.active == nil {
		return nil
	}
	err := w.active.Close()
	w.active = nil
	if err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	return nil
}

func recoverFile(path string, allowMissing bool) ([]Record, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) && allowMissing {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("open WAL %s: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat WAL %s: %w", path, err)
	}
	if info.Size() == 0 {
		return nil, 0, nil
	}
	header := make([]byte, fileHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil || string(header) != string(fileMagic[:]) {
		if err == nil {
			err = errors.New("unsupported or corrupt WAL format")
		}
		return nil, 0, fmt.Errorf("read WAL header %s: %w", path, err)
	}
	validSize := int64(fileHeaderSize)
	var records []Record
	for {
		record, size, err := readRecord(file)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("read WAL %s at offset %d: %w", path, validSize, err)
		}
		records = append(records, record)
		validSize += int64(size)
	}
	return records, validSize, nil
}

func openActive(path string, validSize int64) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open active WAL: %w", err)
	}
	fail := true
	defer func() {
		if fail {
			_ = file.Close()
		}
	}()
	if validSize == 0 {
		if err := file.Truncate(0); err != nil {
			return nil, fmt.Errorf("initialize WAL: %w", err)
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek WAL: %w", err)
		}
		if err := writeAll(file, fileMagic[:]); err != nil {
			return nil, fmt.Errorf("write WAL header: %w", err)
		}
	} else {
		info, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat active WAL: %w", err)
		}
		if info.Size() != validSize {
			if err := file.Truncate(validSize); err != nil {
				return nil, fmt.Errorf("truncate WAL tail: %w", err)
			}
		}
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync active WAL: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("sync WAL directory: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}
	fail = false
	return file, nil
}

func segmentID(path string) uint64 {
	name := filepath.Base(path)
	id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal_"), ".log"), 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

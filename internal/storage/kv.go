package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type IndexRecord struct {
	SSTFile string // 数据所在的文件名
	Offset  int64  // 数据在文件的哪一个字节开始
	Size    int    // 数据的长度
}
type KVStore struct {
	mu sync.RWMutex

	memTable   map[string]string
	memSize    int
	maxMemSize int

	lastFlushTime time.Time
	maxFlushTime  time.Duration

	walFile   *os.File
	index     map[string]IndexRecord
	nextSSTId int
}

func NewKVStore(walPath string) (*KVStore, error) {
	kv := &KVStore{
		memTable:      make(map[string]string),
		maxMemSize:    100,
		index:         make(map[string]IndexRecord),
		lastFlushTime: time.Now(),
		maxFlushTime:  10 * time.Second,
	}

	// 扫描历史冷数据，重建内存中 Index 地图
	kv.loadIndexFromSSTables()
	// 扫描 WAL，恢复还没有落盘的内容
	kv.loadFromWAL(walPath)

	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("打开 WAL 失败：%v", err)
	}
	kv.walFile = file

	go kv.backgroundFlusher() // 后台跑落盘协程

	return kv, nil
}

func (kv *KVStore) loadIndexFromSSTables() {
	files, err := filepath.Glob("data_*.sst")
	if err != nil || len(files) == 0 {
		return
	}

	maxId := -1
	for _, filename := range files {
		parts := strings.Split(strings.TrimPrefix(filename, "data_"), ".")
		if len(parts) > 0 {
			id, _ := strconv.Atoi(parts[0])
			if id > maxId {
				maxId = id
			}
		}

		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		var currentOffset int64 = 0
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text() + "\n"
			size := len(line)

			kvParts := strings.SplitN(strings.TrimSuffix(line, "\n"), ",", 2)
			if len(kvParts) == 2 {
				key := kvParts[0]
				kv.index[key] = IndexRecord{
					SSTFile: filename,
					Offset:  currentOffset,
					Size:    size,
				}
			}
			currentOffset += int64(size)
		}
		file.Close()
		fmt.Printf("[引擎启动] 成功从 %s 重建索引\n", filename)
	}
	kv.nextSSTId = maxId + 1
}

// 使用 WAL 恢复还未落盘的热数据
func (kv *KVStore) loadFromWAL(walPath string) {
	file, err := os.Open(walPath)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ",", 3)
		if len(parts) == 3 && parts[0] == "PUT" {
			key, value := parts[1], parts[2]
			if _, exist := kv.memTable[key]; exist {
				kv.memSize++
			}
			kv.memTable[key] = value
			count++
		}
	}
	if count > 0 {
		fmt.Printf("[引擎启动] 从 WAL 恢复了 %d 条未落盘的热数据\n", count)
	}
}

func (kv *KVStore) Put(key, value string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.memSize >= kv.maxMemSize {
		if err := kv.flush(); err != nil {
			return err
		}
	}

	logLine := fmt.Sprintf("PUT,%s,%s\n", key, value)
	kv.walFile.WriteString(logLine)
	kv.walFile.Sync()

	if _, exist := kv.memTable[key]; !exist {
		kv.memSize++
	}
	kv.memTable[key] = value
	return nil
}

// Delete 墓碑机制减少 I/O 操作
func (kv *KVStore) Delete(key string) error {
	return kv.Put(key, "<TOMBSTONE>")
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if val, exist := kv.memTable[key]; exist {
		if val == "<TOMBSTONE>" {
			return "", false
		}
		return val, true
	}

	record, ok := kv.index[key]
	if !ok {
		return "", false
	}

	file, err := os.Open(record.SSTFile)
	if err != nil {
		return "", false
	}
	defer file.Close()

	file.Seek(record.Offset, 0)
	buffer := make([]byte, record.Size)
	file.Read(buffer)

	line := string(buffer)
	parts := strings.SplitN(strings.TrimSuffix(line, "\n"), ",", 2)
	if len(parts) == 2 {
		value := parts[1]
		if value == "<TOMBSTONE>" {
			return "", false
		}
		return value, true
	}
	return "", false
}

func (kv *KVStore) flush() error {
	if kv.memSize == 0 {
		return nil
	}

	sstFile := fmt.Sprintf("data_%d.sst", kv.nextSSTId)
	file, err := os.Create(sstFile)
	if err != nil {
		return err
	}
	defer file.Close()

	keys := make([]string, len(kv.memTable))
	for k := range kv.memTable {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var currentOffset int64 = 0
	for _, key := range keys {
		v := kv.memTable[key]
		line := fmt.Sprintf("%s,%s\n", key, v)
		n, _ := file.WriteString(line)

		kv.index[key] = IndexRecord{
			SSTFile: sstFile,
			Offset:  currentOffset,
			Size:    n,
		}
		currentOffset += int64(n)
	}

	kv.nextSSTId++
	kv.memTable = make(map[string]string)
	kv.memSize = 0

	kv.walFile.Truncate(0)
	kv.walFile.Seek(0, 0)
	kv.lastFlushTime = time.Now()

	fmt.Printf("[存储引擎] 成功生成 SSTable: %s\n", sstFile)
	return nil
}

// backgroundFlusher 后台巡逻
func (kv *KVStore) backgroundFlusher() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		kv.mu.Lock()
		if kv.memSize > 0 && time.Since(kv.lastFlushTime) >= kv.maxFlushTime {
			fmt.Println("[后台巡逻] 触发时间阈值，强制生成冷数据...")
			kv.flush()
		}
		kv.mu.Unlock()
	}
}

// Compact 数据合并与垃圾回收
func (kv *KVStore) Compact() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.memSize > 0 {
		if err := kv.flush(); err != nil {
			return err
		}
	}

	files, err := filepath.Glob("data_*.sst")
	if err != nil {
		return err
	}

	// 用于暂存所有有效数据的临时切片
	type kvPair struct{ key, val string }
	var validData []kvPair

	// 顺序读取所有旧文件，过滤出有效数据
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		scanner := bufio.NewScanner(file)
		// 逐行读取旧文件
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, ",", 2)

			if len(parts) == 2 {
				key := parts[0]
				val := parts[1]

				// 👑 史诗级修复：抛弃脆弱的 Offset 比较，只根据“全局最新文件名”过滤！
				record, exists := kv.index[key]
				if exists && record.SSTFile == filename {
					if val != "<TOMBSTONE>" {
						validData = append(validData, kvPair{key, val})
					}
				}
			}
		}
		file.Close()
		// 读完立刻无情删除旧文件
		os.Remove(filename)
	}

	// 如果没有拿到有效数据（比如全被删了），直接返回
	if len(validData) == 0 {
		fmt.Println("[存储引擎] 垃圾回收完成！没有有效数据需要保留。")
		return nil
	}

	// 3. 核心排序：对收集到的所有有效数据进行全局字典序排序
	sort.Slice(validData, func(i, j int) bool {
		return validData[i].key < validData[j].key
	})

	// 4. 将全局排序后的数据写入一个全新的超级 SSTable
	compactFileName := fmt.Sprintf("data_%d.sst", kv.nextSSTId)
	compactFile, err := os.Create(compactFileName)
	if err != nil {
		return err
	}
	defer compactFile.Close()

	var currentOffset int64 = 0
	newIndex := make(map[string]IndexRecord)

	for _, pair := range validData {
		line := fmt.Sprintf("%s,%s\n", pair.key, pair.val)
		n, _ := compactFile.WriteString(line)

		newIndex[pair.key] = IndexRecord{
			SSTFile: compactFileName,
			Offset:  currentOffset,
			Size:    n,
		}
		currentOffset += int64(n)
	}

	// 将旧坐标本替换为全新的超级坐标本
	kv.index = newIndex
	kv.nextSSTId++

	fmt.Printf("[存储引擎] 垃圾回收完成！已生成全局纯净且排序的文件: %s\n", compactFileName)
	return nil
}

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

// ValueEntry 存储值和时间戳，用于 LWW 冲突解决
type ValueEntry struct {
	Value     string
	Timestamp int64
}

type IndexRecord struct {
	SSTFile string // 数据所在的文件名
	Offset  int64  // 数据在文件的哪一个字节开始
	Size    int    // 数据的长度
}

type KVStore struct {
	mu sync.RWMutex

	memTable   map[string]ValueEntry
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
		memTable:      make(map[string]ValueEntry),
		maxMemSize:    1000,
		index:         make(map[string]IndexRecord),
		lastFlushTime: time.Now(),
		maxFlushTime:  60 * time.Second,
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

			// SSTable 格式: key,value,timestamp\n — 取第一个逗号前的部分作为 key
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
// WAL 格式: PUT,key,value,timestamp\n
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
		parts := strings.SplitN(line, ",", 4)
		if len(parts) >= 3 && parts[0] == "PUT" {
			key, value := parts[1], parts[2]
			var ts int64 = 0
			if len(parts) == 4 {
				ts, _ = strconv.ParseInt(parts[3], 10, 64)
			}
			if _, exist := kv.memTable[key]; !exist {
				kv.memSize++
			}
			kv.memTable[key] = ValueEntry{Value: value, Timestamp: ts}
			count++
		}
	}
	if count > 0 {
		fmt.Printf("[引擎启动] 从 WAL 恢复了 %d 条未落盘的热数据\n", count)
	}
}

// Put 写入数据，携带时间戳
func (kv *KVStore) Put(key, value string, timestamp int64) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.memSize >= kv.maxMemSize {
		if err := kv.flush(); err != nil {
			return err
		}
	}

	// WAL 格式: PUT,key,value,timestamp\n
	logLine := fmt.Sprintf("PUT,%s,%s,%d\n", key, value, timestamp)
	kv.walFile.WriteString(logLine)
	kv.walFile.Sync()

	if _, exist := kv.memTable[key]; !exist {
		kv.memSize++
	}
	kv.memTable[key] = ValueEntry{Value: value, Timestamp: timestamp}
	return nil
}

// Delete 墓碑机制减少 I/O 操作
func (kv *KVStore) Delete(key string, timestamp int64) error {
	return kv.Put(key, "<TOMBSTONE>", timestamp)
}

// Get 读取数据，返回值和时间戳
func (kv *KVStore) Get(key string) (string, int64, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if entry, exist := kv.memTable[key]; exist {
		if entry.Value == "<TOMBSTONE>" {
			return "", 0, false
		}
		return entry.Value, entry.Timestamp, true
	}

	record, ok := kv.index[key]
	if !ok {
		return "", 0, false
	}

	file, err := os.Open(record.SSTFile)
	if err != nil {
		return "", 0, false
	}
	defer file.Close()

	file.Seek(record.Offset, 0)
	buffer := make([]byte, record.Size)
	file.Read(buffer)

	line := string(buffer)
	// SSTable 格式: key,value,timestamp\n
	parts := strings.SplitN(strings.TrimSuffix(line, "\n"), ",", 3)
	if len(parts) >= 2 {
		value := parts[1]
		var ts int64 = 0
		if len(parts) == 3 {
			ts, _ = strconv.ParseInt(parts[2], 10, 64)
		}
		if value == "<TOMBSTONE>" {
			return "", 0, false
		}
		return value, ts, true
	}
	return "", 0, false
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

	keys := make([]string, 0, len(kv.memTable))
	for k := range kv.memTable {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var currentOffset int64 = 0
	for _, key := range keys {
		entry := kv.memTable[key]
		// SSTable 格式: key,value,timestamp\n
		line := fmt.Sprintf("%s,%s,%d\n", key, entry.Value, entry.Timestamp)
		n, _ := file.WriteString(line)

		kv.index[key] = IndexRecord{
			SSTFile: sstFile,
			Offset:  currentOffset,
			Size:    n,
		}
		currentOffset += int64(n)
	}

	kv.nextSSTId++
	kv.memTable = make(map[string]ValueEntry)
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

		files, _ := filepath.Glob("data_*.sst")
		// SST 文件超过 5 个就自动合并
		if len(files) >= 5 {
			fmt.Println("[后台巡逻] SST 文件过多，触发自动合并...")
			kv.compact()
		}

		kv.mu.Unlock()
	}
}

// Compact 对外暴露的合并接口（加锁）
func (kv *KVStore) Compact() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.compact()
}

// compact 内部合并逻辑
func (kv *KVStore) compact() error {
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
	type kvPair struct {
		key string
		val string
		ts  int64
	}
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
			parts := strings.SplitN(line, ",", 3)

			if len(parts) >= 2 {
				key := parts[0]
				val := parts[1]
				var ts int64 = 0
				if len(parts) == 3 {
					ts, _ = strconv.ParseInt(parts[2], 10, 64)
				}

				// 根据"全局最新文件名"过滤！
				record, exists := kv.index[key]
				if exists && record.SSTFile == filename {
					if val != "<TOMBSTONE>" {
						validData = append(validData, kvPair{key, val, ts})
					}
				}
			}
		}
		file.Close()
		// 读完删除旧文件
		os.Remove(filename)
	}

	// 没有拿到有效数据，直接返回
	if len(validData) == 0 {
		fmt.Println("[存储引擎] 垃圾回收完成！没有有效数据需要保留。")
		return nil
	}

	// 对收集到的所有有效数据进行全局字典序排序
	sort.Slice(validData, func(i, j int) bool {
		return validData[i].key < validData[j].key
	})

	// 将全局排序后的数据写入一个全新的 SSTable
	compactFileName := fmt.Sprintf("data_%d.sst", kv.nextSSTId)
	compactFile, err := os.Create(compactFileName)
	if err != nil {
		return err
	}
	defer compactFile.Close()

	var currentOffset int64 = 0
	newIndex := make(map[string]IndexRecord)

	for _, pair := range validData {
		line := fmt.Sprintf("%s,%s,%d\n", pair.key, pair.val, pair.ts)
		n, _ := compactFile.WriteString(line)

		newIndex[pair.key] = IndexRecord{
			SSTFile: compactFileName,
			Offset:  currentOffset,
			Size:    n,
		}
		currentOffset += int64(n)
	}

	// 将旧坐标本替换为全新的坐标本
	kv.index = newIndex
	kv.nextSSTId++

	fmt.Printf("[存储引擎] 垃圾回收完成！已生成全局纯净且排序的文件: %s\n", compactFileName)
	return nil
}

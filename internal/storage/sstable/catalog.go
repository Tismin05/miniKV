package sstable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Descriptor 是 Engine 所需的文件目录视图，具体命名规则封装在 sstable 包内。
type Descriptor struct {
	Generation int
	Reader     *Reader
}

func FilePath(dir string, generation int) string {
	return filepath.Join(dir, fmt.Sprintf("data_%d.sst", generation))
}

// OpenAll 按 generation 顺序打开目录中的全部 SSTable。
func OpenAll(dir string, cache BlockCache) ([]Descriptor, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "data_*.sst"))
	if err != nil {
		return nil, fmt.Errorf("list SSTables: %w", err)
	}
	sort.Slice(paths, func(i, j int) bool { return generationOf(paths[i]) < generationOf(paths[j]) })
	var descriptors []Descriptor
	for _, path := range paths {
		generation := generationOf(path)
		if generation < 0 {
			continue
		}
		reader, err := Open(path, cache)
		if err != nil {
			for _, descriptor := range descriptors {
				_ = descriptor.Reader.Close()
			}
			return nil, fmt.Errorf("open SSTable %s: %w", path, err)
		}
		descriptors = append(descriptors, Descriptor{Generation: generation, Reader: reader})
	}
	return descriptors, nil
}

// RemoveFiles 在删除全部输入后同步目录；文件内容发布与回收均由 sstable 包处理。
func RemoveFiles(dir string, paths []string) error {
	var result error
	for _, path := range paths {
		if filepath.Dir(path) != dir {
			result = errors.Join(result, fmt.Errorf("refuse to remove SSTable outside data directory: %s", path))
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove SSTable %s: %w", path, err))
		}
	}
	if err := syncDir(dir); err != nil {
		result = errors.Join(result, fmt.Errorf("sync SSTable removals: %w", err))
	}
	return result
}

func generationOf(path string) int {
	name := filepath.Base(path)
	id, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "data_"), ".sst"))
	if err != nil {
		return -1
	}
	return id
}

package router

import (
	"hash/crc32"
	"slices"
	"sort"
	"strconv"
)

type HashRing struct {
	hashKeys []int
	hashMap  map[int]string
	replicas int
}

func NewHashRing(replicas int) *HashRing {
	return &HashRing{
		hashKeys: make([]int, 0, replicas),
		hashMap:  make(map[int]string),
		replicas: replicas,
	}
}

// AddNode 添加虚拟节点和真实节点映射
func (r *HashRing) AddNode(nodeAddr string) {
	for i := 0; i < r.replicas; i++ {
		virtualNodeName := nodeAddr + "-" + strconv.Itoa(i)

		hash := int(crc32.ChecksumIEEE([]byte(virtualNodeName)))

		r.hashKeys = append(r.hashKeys, hash)
		r.hashMap[hash] = nodeAddr
	}
	slices.Sort(r.hashKeys)
}

// GetNodes 根据 Key 计算出它应该去哪几个节点
func (r *HashRing) GetNodes(key string, count int) []string {
	if len(r.hashKeys) == 0 {
		return nil
	}

	// 计算数据的哈希值
	hash := int(crc32.ChecksumIEEE([]byte(key)))

	// 二分查找顺时针寻找第一个大于等于数据哈希值的节点
	index := sort.Search(len(r.hashKeys), func(i int) bool {
		return r.hashKeys[i] >= hash
	})

	nodes := make([]string, 0, count)
	seen := make(map[string]bool)

	for i := 0; len(nodes) < count && i < len(r.hashKeys); i++ {
		currIndex := (index + i) % len(r.hashKeys)
		nodeAddr := r.hashMap[r.hashKeys[currIndex]]

		if !seen[nodeAddr] {
			nodes = append(nodes, nodeAddr)
			seen[nodeAddr] = true
		}
	}
	return nodes
}

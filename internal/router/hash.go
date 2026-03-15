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
		hashKeys: make([]int, replicas),
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

// GetNode 根据 Key 计算出它应该去哪个节点
func (r *HashRing) GetNode(key string) string {
	if len(r.hashKeys) == 0 {
		return ""
	}

	// 计算数据的哈希值
	hash := int(crc32.ChecksumIEEE([]byte(key)))

	// 二分查找顺时针寻找第一个大于等于数据哈希值的节点
	idx := sort.Search(len(r.hashKeys), func(i int) bool {
		return r.hashKeys[i] >= hash
	})

	// 如果到了环的末尾，就回到开头
	if idx == len(r.hashKeys) {
		idx = 0
	}

	return r.hashMap[r.hashKeys[idx]] // 真正的节点地址
}

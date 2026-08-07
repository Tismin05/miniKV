package storage

import "bytes"

// ValueType 表示内部条目携带的操作类型。操作类型独立于 Value 编码，
// 因此任意字节序列（包括旧的 "<TOMBSTONE>" 兼容标记）都可作为用户值。
type ValueType uint8

const (
	ValuePut ValueType = iota + 1
	ValueDelete
)

// InternalEntry 是单节点保存的带版本值。Timestamp 用于跨节点 LWW 比较，
// Sequence 用于单节点内已接受操作的排序，并为未来的 snapshot 预留。
type InternalEntry struct {
	Key       string
	Value     []byte
	Sequence  uint64
	Timestamp int64
	Type      ValueType
}

// ReadResult 通过 Deleted 区分“已删除”和“从未写入”，不向调用方暴露
// 实现相关的 tombstone 特殊值。
type ReadResult struct {
	Value     []byte
	Timestamp int64
	Found     bool
	Deleted   bool
}

// compareVersions 在 candidate 胜出时返回正数。Timestamp 是主要的 LWW
// 版本；时间戳相同则按确定性规则处理：Delete 优先于 Put，两个 Put 再按值的
// 字典序决胜。即使收到时间戳相同的异常写入，副本也能与到达顺序无关地收敛。
func compareVersions(candidate, current InternalEntry) int {
	if candidate.Timestamp > current.Timestamp {
		return 1
	}
	if candidate.Timestamp < current.Timestamp {
		return -1
	}
	if candidate.Type == ValueDelete && current.Type != ValueDelete {
		return 1
	}
	if candidate.Type != ValueDelete && current.Type == ValueDelete {
		return -1
	}
	if candidate.Type == ValuePut {
		return bytes.Compare(candidate.Value, current.Value)
	}
	return 0
}

package compaction

import "sort"

// File 描述 Planner 做输入选择所需的最小 SSTable 信息。
type File struct {
	Path       string
	Generation int
}

// Plan 将 compaction 决策与 Engine 字段和文件操作解耦。
type Plan struct {
	Inputs []File
}

// SelectAll 保留原型阶段的全量压缩策略；M6 可在不修改 Engine 的情况下替换为分层规划。
func SelectAll(files []File) Plan {
	inputs := append([]File(nil), files...)
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Generation < inputs[j].Generation })
	if len(inputs) <= 1 {
		return Plan{}
	}
	return Plan{Inputs: inputs}
}

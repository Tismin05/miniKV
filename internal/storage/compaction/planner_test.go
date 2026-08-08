package compaction

import "testing"

func TestSelectAllReturnsGenerationOrder(t *testing.T) {
	plan := SelectAll([]File{{Path: "three", Generation: 3}, {Path: "one", Generation: 1}, {Path: "two", Generation: 2}})
	if len(plan.Inputs) != 3 || plan.Inputs[0].Path != "one" || plan.Inputs[2].Path != "three" {
		t.Fatalf("plan = %#v", plan)
	}
	if got := SelectAll([]File{{Path: "one", Generation: 1}}); len(got.Inputs) != 0 {
		t.Fatalf("single file should not compact: %#v", got)
	}
}

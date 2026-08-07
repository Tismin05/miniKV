package router

import "testing"

func TestHashRingReturnsDistinctNodes(t *testing.T) {
	ring := NewHashRing(3)
	ring.AddNode("node-a")
	ring.AddNode("node-b")
	nodes := ring.GetNodes("key", 2)
	if len(nodes) != 2 || nodes[0] == nodes[1] {
		t.Fatalf("GetNodes = %v, want two distinct nodes", nodes)
	}
}

package main

import "testing"

func TestNextTimestampIsMonotonic(t *testing.T) {
	previous := nextTimestamp()
	for range 100 {
		current := nextTimestamp()
		if current <= previous {
			t.Fatalf("timestamp = %d, previous = %d", current, previous)
		}
		previous = current
	}
}

package db

import (
	"testing"
)

func TestKvLess(t *testing.T) {
	x := NewKv("x", "123456789")
	y := NewKv("y", "987654321")
	if !KvLess(x, y) {
		t.Fatalf("KV for x should be less than KV for y")
	}

	z := NewKv("z", "123")
	if !KvLess(x, z) {
		t.Fatalf("KV for x should be less than KV for y")
	}
}

package db

import (
	"testing"
)

func TestKvLess(t *testing.T) {
	x := newKvpair("x", "123456789")
	y := newKvpair("y", "987654321")
	if !kvpairLess(x, y) {
		t.Fatalf("KV for x should be less than KV for y")
	}

	z := newKvpair("z", "123")
	if !kvpairLess(x, z) {
		t.Fatalf("KV for x should be less than KV for y")
	}
}

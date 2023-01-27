package db

import (
	"testing"
)

func TestDbCold(t *testing.T) {
	datadir := t.TempDir()

	db := NewDb(datadir)

	db.Add("a", "alphabet")
	db.Add("b", "branches")
	db.Add("c", "cardamom")
	db.Add("d", "detritus")

	actual, _ := db.Get("a")
	if actual != "alphabet" {
		t.Fatalf("Expected db[a] = alphabet, got %v", actual)
	}

	actual, _ = db.Get("b")
	if actual != "branches" {
		t.Fatalf("Expected db[b] = branches, got %v", actual)
	}

	db.Add("a", "alpaca")
	actual, _ = db.Get("a")
	if actual != "alpaca" {
		t.Fatalf("Expected db[a] = alpaca, got %v", actual)
	}
}

func TestDbWarm(t *testing.T) {
	datadir := t.TempDir()

	db := NewDb(datadir)

	db.Add("a", "alphabet")
	db.Add("b", "branches")
	db.Add("c", "cardamom")
	db.Add("d", "detritus")
	db.Add("a", "alpaca")

	db.Close()
	db = NewDb(datadir)

	actual, _ := db.Get("a")
	if actual != "alpaca" {
		t.Fatalf("Expected db[a] = alpaca, got %v", actual)
	}

	actual, _ = db.Get("b")
	if actual != "branches" {
		t.Fatalf("Expected db[b] = branches, got %v", actual)
	}
}

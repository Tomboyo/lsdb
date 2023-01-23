package db

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/btree"
)

type kv struct {
	key, value string
	data       []byte
}

func newDataKv(key, value string) kv {
	kbytes := []byte(key)
	vbytes := []byte(value)
	kl := uint64(len(kbytes))
	vl := uint64(len(vbytes))

	// Create a data payload with the following byte signature:
	// key header:   8 bytes indicating length in bytes of key data
	// key data:     variable length
	// value header: 8 bytes indicating length in bytes of value data
	// value data:   variable length
	data := make([]byte, 0, 128+kl+vl)
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, kl)
	data = append(data, buffer...)
	data = append(data, kbytes...)
	binary.LittleEndian.PutUint64(buffer, vl)
	data = append(data, buffer...)
	data = append(data, vbytes...)

	return kv{key, value, data}
}

func newSearchKv(key string) kv {
	return kv{key, "", nil}
}

func less(a, b kv) bool {
	return strings.Compare(a.key, b.key) == -1
}

type memtable struct {
	size    uint
	maxsize uint
	data    btree.BTreeG[kv]
}

func newMemtable() memtable {
	return memtable{0, 256, *btree.NewG(2, less)}
}

func (m *memtable) add(key, value string) {
	new := newDataKv(key, value)
	old, hasOld := m.data.ReplaceOrInsert(new)
	if hasOld {
		m.size = m.size - uint(len(old.data)) + uint(len(new.data))
	} else {
		m.size += uint(len(new.data))
	}
}

func (m memtable) get(key string) (string, bool) {
	value, hasValue := m.data.Get(newSearchKv(key))
	if hasValue {
		return value.value, true
	} else {
		return "", false
	}
}

func (m memtable) serialize() []byte {
	var bytes []byte
	m.data.Descend(func(item kv) bool {
		bytes = append(bytes, item.data...)
		return true
	})
	return bytes
}

type Db struct {
	datadir  string
	memtable memtable
	maxsize  uint
	head     uint
}

// Returns a Db using the given data directory for persistence.
func NewDb(datadir string) Db {
	return Db{
		datadir,
		newMemtable(),
		2,
		0,
	}
}

func (db *Db) Add(key, value string) {
	db.memtable.add(key, value)
	if db.memtable.size >= db.memtable.maxsize {
		err := db.flush()
		if err != nil {
			log.Fatalf("Failed to persist memtable: %v\n", err)
		}
	}
}

func (db Db) Get(key string) (string, bool) {
	value, ok := db.memtable.get(key)
	if ok {
		return value, true
	} else {
		return "", false
	}
}

func (db *Db) flush() error {
	log.Printf("Flushing memtable (%v bytes > %v bytes)\n", db.memtable.size, db.memtable.maxsize)
	os.MkdirAll(db.datadir, 0700)

	f, err := os.OpenFile(
		filepath.Join(db.datadir, fmt.Sprint(db.head, ".log")),
		os.O_CREATE|os.O_WRONLY,
		0600)
	if err != nil {
		return err
	}

	data := db.memtable.serialize()
	bytes, err := f.Write(data)
	if err != nil {
		return err
	}

	log.Printf("Wrote %v bytes to %v", bytes, f.Name())
	db.head += 1
	return nil
}

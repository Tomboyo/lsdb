package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/google/btree"
)

type memtable struct {
	size    uint64
	maxsize uint64
	data    btree.BTreeG[Kv]
}

func newMemtable() memtable {
	return memtable{0, 64, *btree.NewG(2, KvLess)}
}

func (m *memtable) add(key, value string) {
	new := NewKv(key, value)
	old, hasOld := m.data.ReplaceOrInsert(new)
	if hasOld {
		m.size = m.size - old.ByteLen() + new.ByteLen()
	} else {
		m.size += new.ByteLen()
	}
}

func (m memtable) get(key string) (string, bool) {
	kv, hasValue := m.data.Get(NewSearchKv(key))
	if hasValue {
		return string(kv.value), true
	} else {
		return "", false
	}
}

func (m memtable) serialize() []byte {
	var bytes []byte
	m.data.Descend(func(item Kv) bool {
		bytes = append(bytes, item.Marshal()...)
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
		for i := db.head - 1; ; i-- {
			path := db.logFilePath(i)
			log.Printf("Searching for %v in %v", key, path)
			bytes, err := os.ReadFile(path)
			if err != nil {
				log.Fatalf("Failed to open log file %v: %v\n", path, err)
			}
			value, ok := findInLog(key, bytes)
			if ok {
				return value, true
			}
			if i == 0 {
				break
			}
		}
		return "", false
	}
}

func findInLog(key string, log []byte) (string, bool) {
	for offset := 0; offset < len(log); {
		kv, len := UnmarshalKv(log[offset:])
		comp := CompareKeyToKv(key, kv)

		if comp == 0 {
			return kv.ValString(), true
		}

		// Keys are in descending order, so the search is not in this block.
		if comp == 1 {
			return "", false
		}

		offset += int(len)
	}
	return "", false
}

func (db *Db) flush() error {
	log.Printf("Flushing memtable (%v bytes > %v bytes)\n", db.memtable.size, db.memtable.maxsize)
	os.MkdirAll(db.datadir, 0700)

	f, err := os.OpenFile(
		db.logFilePath(db.head),
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
	db.memtable = newMemtable()
	return nil
}

func (db Db) logFilePath(n uint) string {
	return filepath.Join(db.datadir, fmt.Sprint(n, ".log"))
}

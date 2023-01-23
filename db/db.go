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
	// value header: 8 bytes indicating length in bytes of value data
	// key data:     variable length
	// value data:   variable length
	data := make([]byte, 0, 128+kl+vl)
	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, kl)
	data = append(data, buffer...)
	binary.BigEndian.PutUint64(buffer, vl)
	data = append(data, buffer...)
	data = append(data, kbytes...)
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
	return memtable{0, 64, *btree.NewG(2, less)}
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

func getSerialized(key string, logfile []byte) (string, bool) {
	for offset := 0; offset < len(logfile); {
		klo := uint64(offset) // key-length field offset
		vlo := klo + 8        // value-length field offset
		kdo := vlo + 8        // key data field offset

		keyLen := binary.BigEndian.Uint64(logfile[klo:vlo])
		valLen := binary.BigEndian.Uint64(logfile[vlo:kdo])
		vdo := kdo + keyLen // value data field offset
		if keyLen == uint64(len(key)) {
			loggedKey := string(logfile[kdo:vdo])
			if key == loggedKey {
				return string(logfile[vdo : vdo+valLen]), true
			}
		}
		offset = int(vdo + valLen)
	}
	return "", false
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
			value, ok := getSerialized(key, bytes)
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

package db

import (
	"com/github/tomboyo/lsdb/support"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	segfile  string
	memtable memtable
	maxsize  uint64
	head     uint64
	segments []string
}

// Returns a Db using the given data directory for persistence.
func NewDb(datadir string) Db {
	os.MkdirAll(datadir, 0700)

	segfile := filepath.Join(datadir, "segments")
	segments := loadSegments(segfile)
	log.Printf("Recovered segments: %v", segments)

	var head uint64
	if len(segments) > 0 {
		headIdx, err := strconv.ParseUint(segments[0], 10, 64)
		if err != nil {
			log.Fatalf("Unexpected non-uint log file name: %v\n", err)
		}
		head = headIdx + 1
	} else {
		head = 0
	}

	return Db{
		datadir,
		segfile,
		newMemtable(),
		2,
		head,
		segments,
	}
}

func (db *Db) Add(key, value string) {
	db.memtable.add(key, value)
	if db.memtable.size >= db.memtable.maxsize {
		segment, err := db.flush()
		if err != nil {
			log.Fatalf("Failed to persist memtable: %v\n", err)
		}
		db.segments = append(db.segments, segment)
	}
}

func (db Db) Get(key string) (string, bool) {
	value, ok := db.memtable.get(key)
	if ok {
		return value, true
	} else {
		for i := 0; i < len(db.segments); i++ {
			segment := db.logFilePath(db.segments[i])
			log.Printf("Searching for %v in %v", key, segment)
			bytes, err := os.ReadFile(segment)
			if err != nil {
				log.Fatalf("Failed to open log file %v: %v\n", segment, err)
			}
			value, ok := findInLog(key, bytes)
			if ok {
				return value, true
			}
		}
		return "", false
	}
}

func (db Db) Close() error {
	_, err := db.flush()
	return err
}

func loadSegments(segfile string) []string {
	bytes, err := os.ReadFile(segfile)

	if os.IsNotExist(err) {
		return []string{}
	} else if err != nil {
		log.Fatalf("Unable to read segment file: %v\n", err)
	}

	segments := strings.Split(string(bytes), "\n")
	// Segment files always end with a trailing newline. After splitting, this
	// leaves a dangling "", and after reversing, it's at the head of the slice.
	// Skip it.
	return support.Reverse(segments)[1:]
}

func (db Db) addSegment(segment string) error {
	f, err := os.OpenFile(db.segfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(segment + "\n")
	if err != nil {
		return err
	}
	return nil
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

func (db *Db) flush() (string, error) {
	log.Printf("Flushing memtable (%v bytes > %v bytes)\n", db.memtable.size, db.memtable.maxsize)
	newSegment := fmt.Sprint(db.head)
	path := db.logFilePath(newSegment)
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_WRONLY,
		0600)
	if err != nil {
		return path, err
	}

	data := db.memtable.serialize()
	bytes, err := f.Write(data)
	if err != nil {
		return path, err
	}

	err = db.addSegment(newSegment)
	if err != nil {
		return path, err
	}

	log.Printf("Wrote %v bytes to %v", bytes, f.Name())
	db.head += 1
	db.memtable = newMemtable()
	return newSegment, nil
}

func (db Db) logFilePath(s string) string {
	return filepath.Join(db.datadir, s)
}

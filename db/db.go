package db

import (
	"io/fs"
	"os"
)

type Db struct {
	data     fs.FS
	memtable map[string]string
}

// Returns a Db using the given data directory for persistence.
func NewDb(datadir string) Db {
	var dirfs = os.DirFS(datadir)
	return Db{dirfs, make(map[string]string)}
}

func (db *Db) Add(key, value string) {
	db.memtable[key] = value
}

func (db Db) Get(key string) (*string, bool) {
	value, ok := db.memtable[key]
	if ok {
		return &value, true
	} else {
		return nil, false
	}
}

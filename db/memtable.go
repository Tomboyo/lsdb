package db

import "github.com/google/btree"

type memtable struct {
	size    uint64
	maxsize uint64
	data    btree.BTreeG[kvpair]
}

func newMemtable() memtable {
	return memtable{0, 64, *btree.NewG(2, kvpairLess)}
}

func (m *memtable) add(key, value string) {
	new := newKvpair(key, value)
	old, hasOld := m.data.ReplaceOrInsert(new)
	if hasOld {
		m.size = m.size - old.byteLen() + new.byteLen()
	} else {
		m.size += new.byteLen()
	}
}

func (m memtable) get(key string) (string, bool) {
	kv, hasValue := m.data.Get(NewSearchKvpair(key))
	if hasValue {
		return string(kv.value), true
	} else {
		return "", false
	}
}

func (m memtable) serialize() []byte {
	var bytes []byte
	m.data.Descend(func(item kvpair) bool {
		bytes = append(bytes, item.marshal()...)
		return true
	})
	return bytes
}

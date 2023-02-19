package db

import (
	"encoding/binary"
	"strings"
)

type kvpair struct {
	key, value []byte
}

func newKvpair(key, value string) kvpair {
	return kvpair{[]byte(key), []byte(value)}
}

func NewSearchKvpair(key string) kvpair {
	return kvpair{[]byte(key), []byte{}}
}

func (kv kvpair) valString() string {
	// Clone to make sure we don't hold a reference to a large page file
	return strings.Clone(string(kv.value))
}

func (kv kvpair) byteLen() uint64 {
	return uint64(16 + len(kv.key) + len(kv.value))
}

// marshal this Kv to bytes.
func (kv kvpair) marshal() []byte {
	// Create a data payload with the following byte signature:
	// key header:   8 bytes indicating length in bytes of key data
	// value header: 8 bytes indicating length in bytes of value data
	// key data:     variable length
	// value data:   variable length
	kl := uint64(len(kv.key))
	vl := uint64(len(kv.value))
	data := make([]byte, 0, 128+kl+vl)

	buffer := make([]byte, 8)
	binary.BigEndian.PutUint64(buffer, kl)
	data = append(data, buffer...)
	binary.BigEndian.PutUint64(buffer, vl)
	data = append(data, buffer...)

	data = append(data, kv.key...)
	data = append(data, kv.value...)
	return data
}

func unmarshalKvpair(data []byte) (kvpair, uint64) {
	keyLen := binary.BigEndian.Uint64(data[0:8])
	valLen := binary.BigEndian.Uint64(data[8:16])
	valOffset := 16 + keyLen
	key := data[16:valOffset]
	value := data[valOffset : valOffset+valLen]
	return kvpair{key, value}, 16 + keyLen + valLen
}

func compareKeyToKvpair(s string, kv kvpair) int {
	return strings.Compare(s, string(kv.key))
}

// Tests whether a is "less than" b.
func kvpairLess(a, b kvpair) bool {
	return strings.Compare(string(a.key), string(b.key)) == -1
}

package core

import (
	"bytes"
	"encoding/gob"

	bolt "go.etcd.io/bbolt"
)

type indexEntry struct {
	Paths       map[string]struct{}
	Size        int64
	ContentType string
}

func getEntry(b *bolt.Bucket, hash []byte) (*indexEntry, error) {
	v := b.Get(hash)
	if v == nil {
		return nil, nil
	}
	return decodeEntry(v)
}

func putEntry(b *bolt.Bucket, hash []byte, entry *indexEntry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return err
	}
	if err := b.Put(hash, buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func decodeEntry(v []byte) (*indexEntry, error) {
	buf := bytes.NewReader(v)
	var entry indexEntry
	if err := gob.NewDecoder(buf).Decode(&entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

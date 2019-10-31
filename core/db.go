package core

import (
	"bytes"
	"encoding/gob"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type indexEntry struct {
	Paths       map[string]struct{}
	Size        int64
	ContentType string
}

func getBucketForIndexes(tx *bolt.Tx) (*bolt.Bucket, error) {
	containerKey := []byte("INDEXES")

	if tx.Writable() {
		all, err := tx.CreateBucketIfNotExists(containerKey)
		if err != nil {
			return nil, err
		}
		return all, nil
	}

	all := tx.Bucket(containerKey)
	if all == nil {
		return nil, fmt.Errorf("No indexes have been created")
	}
	return all, nil
}

func getBucketForIndex(tx *bolt.Tx, indexName, subName string) (*bolt.Bucket, error) {
	indexKey := []byte(indexName)
	subKey := []byte(subName)

	all, err := getBucketForIndexes(tx)
	if err != nil {
		return nil, err
	}

	if tx.Writable() {
		b, err := all.CreateBucketIfNotExists(indexKey)
		if err != nil {
			return nil, err
		}

		return b.CreateBucketIfNotExists(subKey)
	}

	b := all.Bucket(indexKey)
	if b == nil {
		return nil, fmt.Errorf("Index %q does not exist", indexName)
	}

	s := b.Bucket(subKey)
	if s == nil {
		return nil, fmt.Errorf("Index is not well-formed")
	}
	return s, nil
}

func deleteBucketForIndex(tx *bolt.Tx, indexName string) error {
	containerKey := []byte("INDEXES")
	indexKey := []byte(indexName)

	all, err := tx.CreateBucketIfNotExists(containerKey)
	if err != nil {
		return err
	}

	return all.DeleteBucket(indexKey)
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

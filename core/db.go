package core

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

const (
	dbPath           = "venn.db"
	dbFileMode       = 0600 // Read/write for owner only
	indexesBucketKey = "INDEXES"
	hashesBucketKey  = "HASHES"
)

var (
	// ErrNotInitialized indicates that the venn database has not been initialized
	ErrNotInitialized = errors.New("venn has not been initialized")
	// ErrNoIndexes indicates that no indexes have been created
	ErrNoIndexes = errors.New("no indexes have been created")
	// ErrIndexNotWellFormed indicates the index structure is corrupted
	ErrIndexNotWellFormed = errors.New("index is not well-formed")
)

// indexEntry represents a file's metadata and locations in the index.
type indexEntry struct {
	// Paths is a set of files with this hash in the index.
	Paths map[string]struct{}

	// Attachments is a map from file extension to path that will be materialized
	// as <hash>.<extension>.
	Attachments map[string]string

	Size        int64
	Timestamp   time.Time
	ContentType string
}

// merge combines another indexEntry into this one, adding all paths and attachments.
func (entry *indexEntry) merge(other *indexEntry) {
	if entry == nil || other == nil {
		return
	}

	for p := range other.Paths {
		entry.Paths[p] = struct{}{}
	}

	for ext, p := range other.Attachments {
		entry.Attachments[ext] = p
	}
}

// CreateDB creates a new venn database file.
func CreateDB(logger hclog.Logger) error {
	db, err := bolt.Open(dbPath, dbFileMode, nil)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	logger.Info("database created successfully", "path", dbPath)
	return nil
}

// getDB opens and returns the venn database.
func getDB() (*bolt.DB, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, ErrNotInitialized
	}

	db, err := bolt.Open(dbPath, dbFileMode, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return db, nil
}

// getBucketForIndexes returns the bucket that contains all indexes.
func getBucketForIndexes(tx *bolt.Tx) (*bolt.Bucket, error) {
	containerKey := []byte(indexesBucketKey)

	if tx.Writable() {
		bucket, err := tx.CreateBucketIfNotExists(containerKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create indexes bucket: %w", err)
		}
		return bucket, nil
	}

	bucket := tx.Bucket(containerKey)
	if bucket == nil {
		return nil, ErrNoIndexes
	}
	return bucket, nil
}

// getBucketForIndex returns the bucket for a specific index and sub-bucket.
func getBucketForIndex(tx *bolt.Tx, indexName, subName string) (*bolt.Bucket, error) {
	if indexName == "" {
		return nil, errors.New("index name cannot be empty")
	}
	if subName == "" {
		return nil, errors.New("sub-bucket name cannot be empty")
	}

	indexKey := []byte(indexName)
	subKey := []byte(subName)

	allBucket, err := getBucketForIndexes(tx)
	if err != nil {
		return nil, err
	}

	if tx.Writable() {
		indexBucket, err := allBucket.CreateBucketIfNotExists(indexKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create index bucket %q: %w", indexName, err)
		}

		subBucket, err := indexBucket.CreateBucketIfNotExists(subKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create sub-bucket %q: %w", subName, err)
		}
		return subBucket, nil
	}

	indexBucket := allBucket.Bucket(indexKey)
	if indexBucket == nil {
		return nil, fmt.Errorf("index %q does not exist", indexName)
	}

	subBucket := indexBucket.Bucket(subKey)
	if subBucket == nil {
		return nil, fmt.Errorf("%w: missing sub-bucket %q", ErrIndexNotWellFormed, subName)
	}
	return subBucket, nil
}

// bucketExistsForIndex checks if a bucket exists for the given index name.
func bucketExistsForIndex(tx *bolt.Tx, indexName string) bool {
	allBucket, err := getBucketForIndexes(tx)
	if err != nil {
		return false
	}

	return allBucket.Bucket([]byte(indexName)) != nil
}

// deleteBucketForIndex deletes the bucket for the given index name.
func deleteBucketForIndex(tx *bolt.Tx, indexName string) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
	}

	containerKey := []byte(indexesBucketKey)
	indexKey := []byte(indexName)

	allBucket, err := tx.CreateBucketIfNotExists(containerKey)
	if err != nil {
		return fmt.Errorf("failed to get indexes bucket: %w", err)
	}

	if err := allBucket.DeleteBucket(indexKey); err != nil {
		return fmt.Errorf("failed to delete index %q: %w", indexName, err)
	}
	return nil
}

// getEntry retrieves an index entry by hash from the bucket.
func getEntry(b *bolt.Bucket, hash []byte) (*indexEntry, error) {
	if b == nil {
		return nil, errors.New("bucket cannot be nil")
	}
	if len(hash) == 0 {
		return nil, errors.New("hash cannot be empty")
	}

	v := b.Get(hash)
	if v == nil {
		return nil, nil
	}
	return decodeEntry(v)
}

// putEntry stores an index entry with the given hash in the bucket.
func putEntry(b *bolt.Bucket, hash []byte, entry *indexEntry) error {
	if b == nil {
		return errors.New("bucket cannot be nil")
	}
	if len(hash) == 0 {
		return errors.New("hash cannot be empty")
	}
	if entry == nil {
		return errors.New("entry cannot be nil")
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}
	if err := b.Put(hash, buf.Bytes()); err != nil {
		return fmt.Errorf("failed to put entry: %w", err)
	}
	return nil
}

// decodeEntry decodes a byte slice into an indexEntry.
func decodeEntry(v []byte) (*indexEntry, error) {
	if len(v) == 0 {
		return nil, errors.New("cannot decode empty data")
	}

	buf := bytes.NewReader(v)
	var entry indexEntry
	if err := gob.NewDecoder(buf).Decode(&entry); err != nil {
		return nil, fmt.Errorf("failed to decode entry: %w", err)
	}
	return &entry, nil
}

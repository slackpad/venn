package core

import (
	"bytes"
	"encoding/gob"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func TestIndexEntry_Merge(t *testing.T) {
	tests := []struct {
		name     string
		entry    *indexEntry
		other    *indexEntry
		wantNil  bool
		wantPath int
		wantAtt  int
	}{
		{
			name:    "merge with nil entry",
			entry:   nil,
			other:   &indexEntry{Paths: map[string]struct{}{"path1": {}}},
			wantNil: true,
		},
		{
			name:    "merge with nil other",
			entry:   &indexEntry{Paths: map[string]struct{}{"path1": {}}},
			other:   nil,
			wantNil: false,
		},
		{
			name: "merge paths and attachments",
			entry: &indexEntry{
				Paths:       map[string]struct{}{"path1": {}},
				Attachments: map[string]string{".json": "meta1.json"},
			},
			other: &indexEntry{
				Paths:       map[string]struct{}{"path2": {}},
				Attachments: map[string]string{".xml": "meta2.xml"},
			},
			wantPath: 2,
			wantAtt:  2,
		},
		{
			name: "merge overlapping paths",
			entry: &indexEntry{
				Paths:       map[string]struct{}{"path1": {}, "path2": {}},
				Attachments: map[string]string{},
			},
			other: &indexEntry{
				Paths:       map[string]struct{}{"path2": {}, "path3": {}},
				Attachments: map[string]string{},
			},
			wantPath: 3,
			wantAtt:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.entry.merge(tt.other)
			if tt.wantNil {
				return
			}
			if len(tt.entry.Paths) != tt.wantPath {
				t.Errorf("merge() paths = %v, want %v", len(tt.entry.Paths), tt.wantPath)
			}
			if len(tt.entry.Attachments) != tt.wantAtt {
				t.Errorf("merge() attachments = %v, want %v", len(tt.entry.Attachments), tt.wantAtt)
			}
		})
	}
}

func TestEncodeDecodeEntry(t *testing.T) {
	now := time.Now().UTC()
	entry := &indexEntry{
		Paths:       map[string]struct{}{"path1": {}, "path2": {}},
		Attachments: map[string]string{".json": "meta.json"},
		Size:        1024,
		Timestamp:   now,
		ContentType: "image/jpeg",
	}

	// Test encoding using gob
	var encodeBuf bytes.Buffer
	enc := gob.NewEncoder(&encodeBuf)
	if err := enc.Encode(entry); err != nil {
		t.Fatalf("failed to encode entry: %v", err)
	}
	encoded := encodeBuf.Bytes()

	// Test decoding
	decoded, err := decodeEntry(encoded)
	if err != nil {
		t.Fatalf("failed to decode entry: %v", err)
	}

	// Verify fields
	if decoded.Size != entry.Size {
		t.Errorf("decoded Size = %v, want %v", decoded.Size, entry.Size)
	}
	if !decoded.Timestamp.Equal(entry.Timestamp) {
		t.Errorf("decoded Timestamp = %v, want %v", decoded.Timestamp, entry.Timestamp)
	}
	if decoded.ContentType != entry.ContentType {
		t.Errorf("decoded ContentType = %v, want %v", decoded.ContentType, entry.ContentType)
	}
	if len(decoded.Paths) != len(entry.Paths) {
		t.Errorf("decoded Paths length = %v, want %v", len(decoded.Paths), len(entry.Paths))
	}
	if len(decoded.Attachments) != len(entry.Attachments) {
		t.Errorf("decoded Attachments length = %v, want %v", len(decoded.Attachments), len(entry.Attachments))
	}
}

func TestDecodeEntry_Errors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "invalid gob data",
			data:    []byte{0xFF, 0xFF, 0xFF},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeEntry(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateDB(t *testing.T) {
	defer func() {
		os.Remove(dbPath)
	}()

	logger := hclog.NewNullLogger()

	// Create database
	err := CreateDB(logger)
	if err != nil {
		t.Fatalf("CreateDB() error = %v", err)
	}

	// Verify database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database file was not created")
	}

	// Clean up
	os.Remove(dbPath)
}

func TestGetDB_NotInitialized(t *testing.T) {
	// Ensure no database file exists
	os.Remove(dbPath)

	_, err := getDB()
	if err != ErrNotInitialized {
		t.Errorf("getDB() error = %v, want %v", err, ErrNotInitialized)
	}
}

func TestBucketOperations(t *testing.T) {
	// Create a test database
	logger := hclog.NewNullLogger()
	err := CreateDB(logger)
	if err != nil {
		t.Fatalf("CreateDB() error = %v", err)
	}
	defer os.Remove(dbPath)

	db, err := getDB()
	if err != nil {
		t.Fatalf("getDB() error = %v", err)
	}
	defer db.Close()

	// Test bucket creation and retrieval
	err = db.Update(func(tx *bolt.Tx) error {
		// Test getBucketForIndex with writable transaction
		bucket, err := getBucketForIndex(tx, "test-index", hashesBucketKey)
		if err != nil {
			return err
		}
		if bucket == nil {
			t.Error("getBucketForIndex() returned nil bucket")
		}

		// Test bucketExistsForIndex
		if !bucketExistsForIndex(tx, "test-index") {
			t.Error("bucketExistsForIndex() = false, want true")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("transaction error = %v", err)
	}

	// Test read-only access
	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "test-index", hashesBucketKey)
		if err != nil {
			return err
		}
		if bucket == nil {
			t.Error("getBucketForIndex() in read tx returned nil bucket")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read transaction error = %v", err)
	}
}

func TestDeleteBucketForIndex(t *testing.T) {
	// Create a test database
	logger := hclog.NewNullLogger()
	err := CreateDB(logger)
	if err != nil {
		t.Fatalf("CreateDB() error = %v", err)
	}
	defer os.Remove(dbPath)

	db, err := getDB()
	if err != nil {
		t.Fatalf("getDB() error = %v", err)
	}
	defer db.Close()

	// Create an index
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := getBucketForIndex(tx, "test-index", hashesBucketKey)
		return err
	})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Delete the index
	err = db.Update(func(tx *bolt.Tx) error {
		return deleteBucketForIndex(tx, "test-index")
	})
	if err != nil {
		t.Fatalf("deleteBucketForIndex() error = %v", err)
	}

	// Verify it's deleted
	err = db.View(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, "test-index") {
			t.Error("index still exists after deletion")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verification error = %v", err)
	}
}

func TestGetEntry_Errors(t *testing.T) {
	tests := []struct {
		name    string
		bucket  *bolt.Bucket
		hash    []byte
		wantErr bool
	}{
		{
			name:    "nil bucket",
			bucket:  nil,
			hash:    []byte("test"),
			wantErr: true,
		},
		{
			name:    "empty hash",
			bucket:  &bolt.Bucket{}, // This would normally fail, but we check first
			hash:    []byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := getEntry(tt.bucket, tt.hash)
			if (err != nil) != tt.wantErr {
				t.Errorf("getEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPutEntry_Errors(t *testing.T) {
	tests := []struct {
		name    string
		bucket  *bolt.Bucket
		hash    []byte
		entry   *indexEntry
		wantErr bool
	}{
		{
			name:    "nil bucket",
			bucket:  nil,
			hash:    []byte("test"),
			entry:   &indexEntry{},
			wantErr: true,
		},
		{
			name:    "empty hash",
			bucket:  &bolt.Bucket{},
			hash:    []byte{},
			entry:   &indexEntry{},
			wantErr: true,
		},
		{
			name:    "nil entry",
			bucket:  &bolt.Bucket{},
			hash:    []byte("test"),
			entry:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := putEntry(tt.bucket, tt.hash, tt.entry)
			if (err != nil) != tt.wantErr {
				t.Errorf("putEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

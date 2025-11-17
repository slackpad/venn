package core

import (
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func setupTestDatabase(t *testing.T) (*bolt.DB, func()) {
	t.Helper()

	logger := hclog.NewNullLogger()
	if err := CreateDB(logger); err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}

	db, err := getDB()
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}

	return db, cleanup
}

func createTestIndex(t *testing.T, db *bolt.DB, indexName string, hashes map[string]*indexEntry) {
	t.Helper()

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, indexName, hashesBucketKey)
		if err != nil {
			return err
		}

		for hashStr, entry := range hashes {
			hash := []byte(hashStr)
			if err := putEntry(bucket, hash, entry); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		t.Fatalf("failed to create test index: %v", err)
	}
}

func TestSetDifference(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create test data
	indexAData := map[string]*indexEntry{
		"hash1": {
			Paths:       map[string]struct{}{"file1.txt": {}},
			Attachments: map[string]string{},
			Size:        100,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash2": {
			Paths:       map[string]struct{}{"file2.txt": {}},
			Attachments: map[string]string{},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash3": {
			Paths:       map[string]struct{}{"file3.txt": {}},
			Attachments: map[string]string{},
			Size:        300,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	indexBData := map[string]*indexEntry{
		"hash2": {
			Paths:       map[string]struct{}{"file2_dup.txt": {}},
			Attachments: map[string]string{},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	createTestIndex(t, db, "indexA", indexAData)
	createTestIndex(t, db, "indexB", indexBData)

	logger := hclog.NewNullLogger()
	err := SetDifference(logger, "result", "indexA", "indexB")
	if err != nil {
		t.Fatalf("SetDifference() error = %v", err)
	}

	// Verify result contains hash1 and hash3, but not hash2
	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "result", hashesBucketKey)
		if err != nil {
			return err
		}

		// Should have hash1
		if bucket.Get([]byte("hash1")) == nil {
			t.Error("result missing hash1")
		}

		// Should not have hash2
		if bucket.Get([]byte("hash2")) != nil {
			t.Error("result contains hash2 (should be excluded)")
		}

		// Should have hash3
		if bucket.Get([]byte("hash3")) == nil {
			t.Error("result missing hash3")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("verification error = %v", err)
	}
}

func TestSetIntersection(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create test data with overlapping entries
	indexAData := map[string]*indexEntry{
		"hash1": {
			Paths:       map[string]struct{}{"fileA1.txt": {}},
			Attachments: map[string]string{},
			Size:        100,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash2": {
			Paths:       map[string]struct{}{"fileA2.txt": {}},
			Attachments: map[string]string{},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	indexBData := map[string]*indexEntry{
		"hash2": {
			Paths:       map[string]struct{}{"fileB2.txt": {}},
			Attachments: map[string]string{},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash3": {
			Paths:       map[string]struct{}{"fileB3.txt": {}},
			Attachments: map[string]string{},
			Size:        300,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	createTestIndex(t, db, "indexA", indexAData)
	createTestIndex(t, db, "indexB", indexBData)

	logger := hclog.NewNullLogger()
	err := SetIntersection(logger, "result", "indexA", "indexB")
	if err != nil {
		t.Fatalf("SetIntersection() error = %v", err)
	}

	// Verify result contains only hash2
	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "result", hashesBucketKey)
		if err != nil {
			return err
		}

		// Should not have hash1
		if bucket.Get([]byte("hash1")) != nil {
			t.Error("result contains hash1 (should be excluded)")
		}

		// Should have hash2
		if bucket.Get([]byte("hash2")) == nil {
			t.Error("result missing hash2")
		}

		// Should not have hash3
		if bucket.Get([]byte("hash3")) != nil {
			t.Error("result contains hash3 (should be excluded)")
		}

		// Verify merged paths for hash2
		entry, err := getEntry(bucket, []byte("hash2"))
		if err != nil {
			return err
		}
		if len(entry.Paths) != 2 {
			t.Errorf("hash2 paths = %v, want 2 (merged)", len(entry.Paths))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("verification error = %v", err)
	}
}

func TestSetUnion(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	indexAData := map[string]*indexEntry{
		"hash1": {
			Paths:       map[string]struct{}{"fileA1.txt": {}},
			Attachments: map[string]string{},
			Size:        100,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash2": {
			Paths:       map[string]struct{}{"fileA2.txt": {}},
			Attachments: map[string]string{".json": "metaA2.json"},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	indexBData := map[string]*indexEntry{
		"hash2": {
			Paths:       map[string]struct{}{"fileB2.txt": {}},
			Attachments: map[string]string{".xml": "metaB2.xml"},
			Size:        200,
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
		"hash3": {
			Paths:       map[string]struct{}{"fileB3.txt": {}},
			Attachments: map[string]string{},
			Size:        300,
			Timestamp:   time.Now(),
			ContentType: "image/jpeg",
		},
	}

	createTestIndex(t, db, "indexA", indexAData)
	createTestIndex(t, db, "indexB", indexBData)

	logger := hclog.NewNullLogger()
	err := SetUnion(logger, "result", "indexA", "indexB")
	if err != nil {
		t.Fatalf("SetUnion() error = %v", err)
	}

	// Verify result contains all hashes
	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "result", hashesBucketKey)
		if err != nil {
			return err
		}

		// Should have hash1
		if bucket.Get([]byte("hash1")) == nil {
			t.Error("result missing hash1")
		}

		// Should have hash2
		if bucket.Get([]byte("hash2")) == nil {
			t.Error("result missing hash2")
		}

		// Should have hash3
		if bucket.Get([]byte("hash3")) == nil {
			t.Error("result missing hash3")
		}

		// Verify merged paths and attachments for hash2
		entry, err := getEntry(bucket, []byte("hash2"))
		if err != nil {
			return err
		}
		if len(entry.Paths) != 2 {
			t.Errorf("hash2 paths = %v, want 2 (merged)", len(entry.Paths))
		}
		if len(entry.Attachments) != 2 {
			t.Errorf("hash2 attachments = %v, want 2 (merged)", len(entry.Attachments))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("verification error = %v", err)
	}
}

func TestSetOperations_Errors(t *testing.T) {
	logger := hclog.NewNullLogger()

	tests := []struct {
		name   string
		target string
		indexA string
		indexB string
	}{
		{
			name:   "empty target",
			target: "",
			indexA: "indexA",
			indexB: "indexB",
		},
		{
			name:   "empty indexA",
			target: "result",
			indexA: "",
			indexB: "indexB",
		},
		{
			name:   "empty indexB",
			target: "result",
			indexA: "indexA",
			indexB: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_difference", func(t *testing.T) {
			err := SetDifference(logger, tt.target, tt.indexA, tt.indexB)
			if err == nil {
				t.Error("SetDifference() expected error for invalid input")
			}
		})

		t.Run(tt.name+"_intersection", func(t *testing.T) {
			err := SetIntersection(logger, tt.target, tt.indexA, tt.indexB)
			if err == nil {
				t.Error("SetIntersection() expected error for invalid input")
			}
		})

		t.Run(tt.name+"_union", func(t *testing.T) {
			err := SetUnion(logger, tt.target, tt.indexA, tt.indexB)
			if err == nil {
				t.Error("SetUnion() expected error for invalid input")
			}
		})
	}
}

func TestSetOperations_TargetExists(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	// Create a target index that already exists
	createTestIndex(t, db, "existing", map[string]*indexEntry{})
	createTestIndex(t, db, "indexA", map[string]*indexEntry{})
	createTestIndex(t, db, "indexB", map[string]*indexEntry{})

	logger := hclog.NewNullLogger()

	t.Run("difference with existing target", func(t *testing.T) {
		err := SetDifference(logger, "existing", "indexA", "indexB")
		if err == nil {
			t.Error("expected error when target index already exists")
		}
	})

	t.Run("intersection with existing target", func(t *testing.T) {
		err := SetIntersection(logger, "existing", "indexA", "indexB")
		if err == nil {
			t.Error("expected error when target index already exists")
		}
	})

	t.Run("union with existing target", func(t *testing.T) {
		err := SetUnion(logger, "existing", "indexA", "indexB")
		if err == nil {
			t.Error("expected error when target index already exists")
		}
	})
}

func TestMergeFunction(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	err := db.Update(func(tx *bolt.Tx) error {
		firstBucket, err := getBucketForIndex(tx, "first", hashesBucketKey)
		if err != nil {
			return err
		}

		secondBucket, err := getBucketForIndex(tx, "second", hashesBucketKey)
		if err != nil {
			return err
		}

		targetBucket, err := getBucketForIndex(tx, "target", hashesBucketKey)
		if err != nil {
			return err
		}

		// Add entries to first bucket
		entry1 := &indexEntry{
			Paths:       map[string]struct{}{"file1.txt": {}},
			Attachments: map[string]string{".json": "meta1.json"},
		}
		if err := putEntry(firstBucket, []byte("hash1"), entry1); err != nil {
			return err
		}

		// Add entries to second bucket
		entry2 := &indexEntry{
			Paths:       map[string]struct{}{"file2.txt": {}},
			Attachments: map[string]string{".xml": "meta2.xml"},
		}
		if err := putEntry(secondBucket, []byte("hash2"), entry2); err != nil {
			return err
		}

		// Merge
		count, err := merge(firstBucket, secondBucket, targetBucket)
		if err != nil {
			return err
		}

		if count != 2 {
			t.Errorf("merge() count = %v, want 2", count)
		}

		// Verify both entries are in target
		if targetBucket.Get([]byte("hash1")) == nil {
			t.Error("target missing hash1")
		}
		if targetBucket.Get([]byte("hash2")) == nil {
			t.Error("target missing hash2")
		}

		return nil
	})

	if err != nil {
		t.Fatalf("merge test error = %v", err)
	}
}

func TestIntersectFunction(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	err := db.Update(func(tx *bolt.Tx) error {
		firstBucket, err := getBucketForIndex(tx, "first", hashesBucketKey)
		if err != nil {
			return err
		}

		secondBucket, err := getBucketForIndex(tx, "second", hashesBucketKey)
		if err != nil {
			return err
		}

		targetBucket, err := getBucketForIndex(tx, "target", hashesBucketKey)
		if err != nil {
			return err
		}

		// Add common entry to both buckets
		entry1 := &indexEntry{
			Paths:       map[string]struct{}{"file1.txt": {}},
			Attachments: map[string]string{},
		}
		if err := putEntry(firstBucket, []byte("common"), entry1); err != nil {
			return err
		}

		entry2 := &indexEntry{
			Paths:       map[string]struct{}{"file2.txt": {}},
			Attachments: map[string]string{},
		}
		if err := putEntry(secondBucket, []byte("common"), entry2); err != nil {
			return err
		}

		// Add unique entry to first bucket
		entry3 := &indexEntry{
			Paths:       map[string]struct{}{"unique.txt": {}},
			Attachments: map[string]string{},
		}
		if err := putEntry(firstBucket, []byte("unique"), entry3); err != nil {
			return err
		}

		// Intersect
		count, err := intersect(firstBucket, secondBucket, targetBucket)
		if err != nil {
			return err
		}

		if count != 1 {
			t.Errorf("intersect() count = %v, want 1", count)
		}

		// Verify only common entry is in target
		if targetBucket.Get([]byte("common")) == nil {
			t.Error("target missing common hash")
		}
		if targetBucket.Get([]byte("unique")) != nil {
			t.Error("target contains unique hash (should be excluded)")
		}

		// Verify paths were merged
		result, err := getEntry(targetBucket, []byte("common"))
		if err != nil {
			return err
		}
		if len(result.Paths) != 2 {
			t.Errorf("intersected entry paths = %v, want 2", len(result.Paths))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("intersect test error = %v", err)
	}
}

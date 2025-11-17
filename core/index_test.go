package core

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func TestDetectContentType(t *testing.T) {
	tmpDir := t.TempDir()
	logger := hclog.NewNullLogger()

	tests := []struct {
		name        string
		content     []byte
		wantType    string
		description string
	}{
		{
			name:        "text file",
			content:     []byte("Hello, this is a text file with enough content to detect type"),
			wantType:    "application/octet-stream", // http.DetectContentType doesn't always detect plain text
			description: "plain text content",
		},
		{
			name: "HTML file",
			content: []byte(`<!DOCTYPE html>
<html>
<head><title>Test</title></head>
<body><p>Hello</p></body>
</html>` + strings.Repeat(" ", 500)),
			wantType:    "text/html",
			description: "HTML content",
		},
		{
			name:        "small file",
			content:     []byte("small"),
			wantType:    defaultContentType,
			description: "file smaller than detection threshold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(tmpDir, tt.name)
			if err := os.WriteFile(filePath, tt.content, 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			f, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("failed to open test file: %v", err)
			}
			defer f.Close()

			info, err := f.Stat()
			if err != nil {
				t.Fatalf("failed to stat file: %v", err)
			}

			gotType, err := detectContentType(logger, f, info)
			if err != nil {
				t.Fatalf("detectContentType() error = %v", err)
			}

			if gotType != tt.wantType {
				t.Errorf("detectContentType() = %v, want %v (%s)", gotType, tt.wantType, tt.description)
			}
		})
	}
}

func TestCountFiles(t *testing.T) {
	tmpDir := t.TempDir()
	logger := hclog.NewNullLogger()

	// Create test directory structure
	testFiles := []string{
		"file1.txt",
		"file2.txt",
		"subdir/file3.txt",
		"subdir/file4.txt",
		"subdir/nested/file5.txt",
	}

	for _, file := range testFiles {
		filePath := filepath.Join(tmpDir, file)
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	count, err := countFiles(logger, tmpDir)
	if err != nil {
		t.Fatalf("countFiles() error = %v", err)
	}

	if count != len(testFiles) {
		t.Errorf("countFiles() = %v, want %v", count, len(testFiles))
	}
}

func TestCountFiles_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	logger := hclog.NewNullLogger()

	count, err := countFiles(logger, tmpDir)
	if err != nil {
		t.Fatalf("countFiles() error = %v", err)
	}

	if count != 0 {
		t.Errorf("countFiles() = %v, want 0", count)
	}
}

func TestCountFiles_NonExistentPath(t *testing.T) {
	logger := hclog.NewNullLogger()

	_, err := countFiles(logger, "/nonexistent/path")
	if err == nil {
		t.Error("countFiles() expected error for nonexistent path")
	}
}

func TestIndexAddFiles(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// Create test files
	testFiles := map[string]string{
		"file1.txt": "content of file 1",
		"file2.txt": "content of file 2",
	}

	for name, content := range testFiles {
		filePath := filepath.Join(tmpDir, name)
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
	}

	logger := hclog.NewNullLogger()
	err := IndexAddFiles(logger, "test-index", tmpDir)
	if err != nil {
		t.Fatalf("IndexAddFiles() error = %v", err)
	}

	// Verify files were indexed
	db, err := getDB()
	if err != nil {
		t.Fatalf("failed to open database for verification: %v", err)
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "test-index", hashesBucketKey)
		if err != nil {
			return err
		}

		// Count entries
		count := 0
		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			count++
		}

		if count != len(testFiles) {
			t.Errorf("indexed %v files, want %v", count, len(testFiles))
		}

		return nil
	})

	if err != nil {
		t.Fatalf("verification error = %v", err)
	}
}

func TestIndexAddFiles_Errors(t *testing.T) {
	logger := hclog.NewNullLogger()

	tests := []struct {
		name      string
		indexName string
		rootPath  string
		wantErr   bool
	}{
		{
			name:      "empty index name",
			indexName: "",
			rootPath:  "/tmp",
			wantErr:   true,
		},
		{
			name:      "empty root path",
			indexName: "test",
			rootPath:  "",
			wantErr:   true,
		},
		{
			name:      "nonexistent path",
			indexName: "test",
			rootPath:  "/nonexistent/path",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IndexAddFiles(logger, tt.indexName, tt.rootPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("IndexAddFiles() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIndexCat(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	// Create test index with data
	hash := sha256.Sum256([]byte("test content"))
	indexData := map[string]*indexEntry{
		string(hash[:]): {
			Paths:       map[string]struct{}{"/path/to/file.txt": {}},
			Attachments: map[string]string{},
			Size:        100,
			Timestamp:   mustParseTime(t, "2024-01-01T12:00:00Z"),
			ContentType: "text/plain",
		},
	}

	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()
		createTestIndex(t, db, "test-index", indexData)
	}()

	logger := hclog.NewNullLogger()

	// Redirect stdout to capture output (in a real test environment)
	// For now, just verify it doesn't error
	err := IndexCat(logger, "test-index")
	if err != nil {
		t.Fatalf("IndexCat() error = %v", err)
	}
}

func TestIndexCat_EmptyIndexName(t *testing.T) {
	logger := hclog.NewNullLogger()

	err := IndexCat(logger, "")
	if err == nil {
		t.Error("IndexCat() expected error for empty index name")
	}
}

func TestIndexChunk(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	// Create test index with multiple entries
	indexData := make(map[string]*indexEntry)
	for i := 0; i < 10; i++ {
		hash := sha256.Sum256([]byte(fmt.Sprintf("content%d", i)))
		indexData[string(hash[:])] = &indexEntry{
			Paths:       map[string]struct{}{fmt.Sprintf("/file%d.txt", i): {}},
			Attachments: map[string]string{},
			Size:        int64(100 + i),
			Timestamp:   mustParseTime(t, "2024-01-01T12:00:00Z"),
			ContentType: "text/plain",
		}
	}

	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()
		createTestIndex(t, db, "source-index", indexData)
	}()

	logger := hclog.NewNullLogger()
	err := IndexChunk(logger, "source-index", "chunk", 3)
	if err != nil {
		t.Fatalf("IndexChunk() error = %v", err)
	}

	// Verify chunks were created
	// With 10 entries and chunk size 3, we expect 4 chunks (3+3+3+1)
	expectedChunks := []string{"chunk-0", "chunk-1", "chunk-2", "chunk-3"}

	for _, chunkName := range expectedChunks {
		func() {
			db, err := getDB()
			if err != nil {
				t.Fatalf("failed to open database for verification: %v", err)
			}
			defer db.Close()
			err = db.View(func(tx *bolt.Tx) error {
				if !bucketExistsForIndex(tx, chunkName) {
					t.Errorf("expected chunk %q to exist", chunkName)
				}
				return nil
			})
			if err != nil {
				t.Fatalf("verification error = %v", err)
			}
		}()
	}
}

func TestIndexChunk_Errors(t *testing.T) {
	logger := hclog.NewNullLogger()

	tests := []struct {
		name        string
		indexName   string
		targetName  string
		chunkSize   int
		wantErr     bool
		description string
	}{
		{
			name:        "empty index name",
			indexName:   "",
			targetName:  "target",
			chunkSize:   10,
			wantErr:     true,
			description: "index name cannot be empty",
		},
		{
			name:        "empty target prefix",
			indexName:   "source",
			targetName:  "",
			chunkSize:   10,
			wantErr:     true,
			description: "target prefix cannot be empty",
		},
		{
			name:        "zero chunk size",
			indexName:   "source",
			targetName:  "target",
			chunkSize:   0,
			wantErr:     true,
			description: "chunk size must be positive",
		},
		{
			name:        "negative chunk size",
			indexName:   "source",
			targetName:  "target",
			chunkSize:   -1,
			wantErr:     true,
			description: "chunk size must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := IndexChunk(logger, tt.indexName, tt.targetName, tt.chunkSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("IndexChunk() error = %v, wantErr %v (%s)", err, tt.wantErr, tt.description)
			}
		})
	}
}

func TestIndexList(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	// Create multiple indexes
	indexNames := []string{"index1", "index2", "index3"}
	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()
		for _, name := range indexNames {
			createTestIndex(t, db, name, map[string]*indexEntry{})
		}
	}()

	logger := hclog.NewNullLogger()

	// Just verify it doesn't error (output goes to stdout)
	err := IndexList(logger)
	if err != nil {
		t.Fatalf("IndexList() error = %v", err)
	}
}

func TestIndexStats(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	// Create test index with various content types
	hash1 := sha256.Sum256([]byte("content1"))
	hash2 := sha256.Sum256([]byte("content2"))
	hash3 := sha256.Sum256([]byte("content3"))

	indexData := map[string]*indexEntry{
		string(hash1[:]): {
			Paths:       map[string]struct{}{"/file1.txt": {}, "/file1_dup.txt": {}},
			Attachments: map[string]string{},
			Size:        100,
			Timestamp:   mustParseTime(t, "2024-01-01T12:00:00Z"),
			ContentType: "text/plain",
		},
		string(hash2[:]): {
			Paths:       map[string]struct{}{"/file2.jpg": {}},
			Attachments: map[string]string{},
			Size:        200,
			Timestamp:   mustParseTime(t, "2024-01-01T12:00:00Z"),
			ContentType: "image/jpeg",
		},
		string(hash3[:]): {
			Paths:       map[string]struct{}{"/file3.txt": {}},
			Attachments: map[string]string{},
			Size:        150,
			Timestamp:   mustParseTime(t, "2024-01-01T12:00:00Z"),
			ContentType: "text/plain",
		},
	}

	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()
		createTestIndex(t, db, "test-index", indexData)
	}()

	logger := hclog.NewNullLogger()
	err := IndexStats(logger, "test-index")
	if err != nil {
		t.Fatalf("IndexStats() error = %v", err)
	}
}

func TestIndexStats_EmptyIndexName(t *testing.T) {
	logger := hclog.NewNullLogger()

	err := IndexStats(logger, "")
	if err == nil {
		t.Error("IndexStats() expected error for empty index name")
	}
}

func TestIndexDelete(t *testing.T) {
	cleanup := initTestDatabase(t)
	defer cleanup()

	// Create test index
	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database: %v", err)
		}
		defer db.Close()
		createTestIndex(t, db, "test-index", map[string]*indexEntry{})
	}()

	logger := hclog.NewNullLogger()
	err := IndexDelete(logger, "test-index")
	if err != nil {
		t.Fatalf("IndexDelete() error = %v", err)
	}

	// Verify index was deleted
	func() {
		db, err := getDB()
		if err != nil {
			t.Fatalf("failed to open database for verification: %v", err)
		}
		defer db.Close()
		err = db.View(func(tx *bolt.Tx) error {
			if bucketExistsForIndex(tx, "test-index") {
				t.Error("index still exists after deletion")
			}
			return nil
		})

		if err != nil {
			t.Fatalf("verification error = %v", err)
		}
	}()
}

func TestIndexDelete_EmptyIndexName(t *testing.T) {
	logger := hclog.NewNullLogger()

	err := IndexDelete(logger, "")
	if err == nil {
		t.Error("IndexDelete() expected error for empty index name")
	}
}

func TestMakeFileEntry(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	tmpDir := t.TempDir()
	logger := hclog.NewNullLogger()

	// Create test file
	content := []byte("test file content for hashing")
	filePath := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat test file: %v", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, "test-index", hashesBucketKey)
		if err != nil {
			return err
		}

		hash, entry, err := makeFileEntry(logger, bucket, filePath, info)
		if err != nil {
			t.Fatalf("makeFileEntry() error = %v", err)
		}

		// Verify hash is correct
		expectedHash := sha256.Sum256(content)
		if string(hash) != string(expectedHash[:]) {
			t.Errorf("hash mismatch")
		}

		// Verify entry fields
		if entry.Size != int64(len(content)) {
			t.Errorf("entry.Size = %v, want %v", entry.Size, len(content))
		}

		if _, exists := entry.Paths[filePath]; !exists {
			t.Errorf("entry.Paths missing %q", filePath)
		}

		if entry.ContentType != "application/octet-stream" {
			t.Errorf("entry.ContentType = %v, want application/octet-stream", entry.ContentType)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("transaction error = %v", err)
	}
}

// Helper function to parse time for tests
func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("failed to parse time %q: %v", s, err)
	}
	return ts
}

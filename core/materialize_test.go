package core

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

func TestCopyFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a source file
	srcPath := filepath.Join(tmpDir, "source.txt")
	content := []byte("test content for copy")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	dstPath := filepath.Join(tmpDir, "destination.txt")

	// Test successful copy
	err := copyFile(srcPath, dstPath)
	if err != nil {
		t.Fatalf("copyFile() error = %v", err)
	}

	// Verify destination file exists and has correct content
	dstContent, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if string(dstContent) != string(content) {
		t.Errorf("destination content = %q, want %q", dstContent, content)
	}
}

func TestCopyFile_Errors(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		src     string
		dst     string
		wantErr bool
	}{
		{
			name:    "empty source",
			src:     "",
			dst:     filepath.Join(tmpDir, "dst.txt"),
			wantErr: true,
		},
		{
			name:    "empty destination",
			src:     filepath.Join(tmpDir, "src.txt"),
			dst:     "",
			wantErr: true,
		},
		{
			name:    "nonexistent source",
			src:     filepath.Join(tmpDir, "nonexistent.txt"),
			dst:     filepath.Join(tmpDir, "dst.txt"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := copyFile(tt.src, tt.dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("copyFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCopyFileWithHash(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a source file
	content := []byte("test content for hash copy")
	srcPath := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Compute hash
	h := sha256.New()
	h.Write(content)
	hash := h.Sum(nil)

	dstPath := filepath.Join(tmpDir, "destination.txt")
	timestamp := time.Now().Add(-24 * time.Hour)

	// Test successful copy with hash verification
	err := copyFileWithHash(hash, srcPath, dstPath, timestamp)
	if err != nil {
		t.Fatalf("copyFileWithHash() error = %v", err)
	}

	// Verify destination file exists
	dstContent, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if string(dstContent) != string(content) {
		t.Errorf("destination content = %q, want %q", dstContent, content)
	}

	// Verify timestamp was set
	info, err := os.Stat(dstPath)
	if err != nil {
		t.Fatalf("failed to stat destination file: %v", err)
	}

	// Allow for small time differences due to filesystem precision
	if info.ModTime().Sub(timestamp).Abs() > time.Second {
		t.Errorf("destination timestamp = %v, want %v", info.ModTime(), timestamp)
	}
}

func TestCopyFileWithHash_HashMismatch(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a source file
	content := []byte("test content")
	srcPath := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Use wrong hash
	wrongHash := make([]byte, 32)
	for i := range wrongHash {
		wrongHash[i] = 0xFF
	}

	dstPath := filepath.Join(tmpDir, "destination.txt")
	timestamp := time.Now()

	// Test that hash mismatch is detected
	err := copyFileWithHash(wrongHash, srcPath, dstPath, timestamp)
	if err == nil {
		t.Error("copyFileWithHash() expected error for hash mismatch")
	}

	// Verify destination file was not created (should be cleaned up)
	if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
		t.Error("destination file should not exist after failed copy")
	}
}

func TestCopyFileWithHash_Errors(t *testing.T) {
	tmpDir := t.TempDir()

	validHash := make([]byte, 32)

	tests := []struct {
		name    string
		hash    []byte
		src     string
		dst     string
		wantErr bool
	}{
		{
			name:    "empty hash",
			hash:    []byte{},
			src:     filepath.Join(tmpDir, "src.txt"),
			dst:     filepath.Join(tmpDir, "dst.txt"),
			wantErr: true,
		},
		{
			name:    "empty source",
			hash:    validHash,
			src:     "",
			dst:     filepath.Join(tmpDir, "dst.txt"),
			wantErr: true,
		},
		{
			name:    "empty destination",
			hash:    validHash,
			src:     filepath.Join(tmpDir, "src.txt"),
			dst:     "",
			wantErr: true,
		},
		{
			name:    "nonexistent source",
			hash:    validHash,
			src:     filepath.Join(tmpDir, "nonexistent.txt"),
			dst:     filepath.Join(tmpDir, "dst.txt"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := copyFileWithHash(tt.hash, tt.src, tt.dst, time.Now())
			if (err != nil) != tt.wantErr {
				t.Errorf("copyFileWithHash() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMaterialize(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// Create source files
	file1Content := []byte("file 1 content")
	file1Path := filepath.Join(tmpDir, "file1.txt")
	if err := os.WriteFile(file1Path, file1Content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	file2Content := []byte("file 2 content")
	file2Path := filepath.Join(tmpDir, "file2.jpg")
	if err := os.WriteFile(file2Path, file2Content, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Compute hashes
	h1 := sha256.New()
	h1.Write(file1Content)
	hash1 := h1.Sum(nil)

	h2 := sha256.New()
	h2.Write(file2Content)
	hash2 := h2.Sum(nil)

	// Create index with test data
	now := time.Now()
	indexData := map[string]*indexEntry{
		string(hash1): {
			Paths:       map[string]struct{}{file1Path: {}},
			Attachments: map[string]string{},
			Size:        int64(len(file1Content)),
			Timestamp:   now,
			ContentType: "text/plain",
		},
		string(hash2): {
			Paths:       map[string]struct{}{file2Path: {}},
			Attachments: map[string]string{},
			Size:        int64(len(file2Content)),
			Timestamp:   now,
			ContentType: "image/jpeg",
		},
	}

	createTestIndex(t, db, "test-index", indexData)

	// Materialize the index
	outputDir := filepath.Join(tmpDir, "output")
	logger := hclog.NewNullLogger()
	err := Materialize(logger, "test-index", outputDir)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}

	// Verify materialized files exist
	// Files should be organized as: output/{first_byte}/{second_byte}/{full_hash}.ext

	// Verify output dir was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Error("output directory was not created")
	}

	// Verify some subdirectories were created
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatalf("failed to read output directory: %v", err)
	}

	if len(entries) == 0 {
		t.Error("no subdirectories created in output directory")
	}
}

func TestMaterialize_Errors(t *testing.T) {
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
			rootPath:  "/tmp/output",
			wantErr:   true,
		},
		{
			name:      "empty root path",
			indexName: "test-index",
			rootPath:  "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Materialize(logger, tt.indexName, tt.rootPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("Materialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMaterialize_WithAttachments(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// Create source file and attachment
	fileContent := []byte("photo content")
	filePath := filepath.Join(tmpDir, "photo.jpg")
	if err := os.WriteFile(filePath, fileContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	attachmentContent := []byte(`{"title": "photo"}`)
	attachmentPath := filepath.Join(tmpDir, "photo.jpg.json")
	if err := os.WriteFile(attachmentPath, attachmentContent, 0644); err != nil {
		t.Fatalf("failed to create attachment: %v", err)
	}

	// Compute hash
	h := sha256.New()
	h.Write(fileContent)
	hash := h.Sum(nil)

	// Create index with attachment
	indexData := map[string]*indexEntry{
		string(hash): {
			Paths: map[string]struct{}{filePath: {}},
			Attachments: map[string]string{
				".json": attachmentPath,
			},
			Size:        int64(len(fileContent)),
			Timestamp:   time.Now(),
			ContentType: "image/jpeg",
		},
	}

	createTestIndex(t, db, "test-index", indexData)

	// Materialize
	outputDir := filepath.Join(tmpDir, "output")
	logger := hclog.NewNullLogger()
	err := Materialize(logger, "test-index", outputDir)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}

	// Verify output directory was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Error("output directory was not created")
	}
}

func TestMaterialize_SkipExisting(t *testing.T) {
	db, cleanup := setupTestDatabase(t)
	defer cleanup()

	tmpDir := t.TempDir()

	// Create source file
	fileContent := []byte("test content")
	filePath := filepath.Join(tmpDir, "file.txt")
	if err := os.WriteFile(filePath, fileContent, 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Compute hash
	h := sha256.New()
	h.Write(fileContent)
	hash := h.Sum(nil)

	// Create index
	indexData := map[string]*indexEntry{
		string(hash): {
			Paths:       map[string]struct{}{filePath: {}},
			Attachments: map[string]string{},
			Size:        int64(len(fileContent)),
			Timestamp:   time.Now(),
			ContentType: "text/plain",
		},
	}

	createTestIndex(t, db, "test-index", indexData)

	// First materialization
	outputDir := filepath.Join(tmpDir, "output")
	logger := hclog.NewNullLogger()
	err := Materialize(logger, "test-index", outputDir)
	if err != nil {
		t.Fatalf("first Materialize() error = %v", err)
	}

	// Second materialization (should skip existing files)
	err = Materialize(logger, "test-index", outputDir)
	if err != nil {
		t.Fatalf("second Materialize() error = %v", err)
	}
}

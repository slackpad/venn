package core

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

const (
	materializedDirMode  = 0755
	materializedFileMode = 0644
)

// Materialize creates a materialized view of an index in the given directory.
func Materialize(logger hclog.Logger, indexName, rootPath string) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
	}
	if rootPath == "" {
		return errors.New("root path cannot be empty")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, indexName, hashesBucketKey)
		if err != nil {
			return err
		}

		bar := pb.StartNew(bucket.Stats().KeyN)
		defer bar.Finish()

		cursor := bucket.Cursor()
		for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
			bar.Increment()

			entry, err := decodeEntry(entryData)
			if err != nil {
				return fmt.Errorf("failed to decode entry: %w", err)
			}

			// Create directory structure based on first two bytes of hash
			dir := filepath.Join(rootPath, fmt.Sprintf("%02x", hash[0]), fmt.Sprintf("%02x", hash[1]))
			if err := os.MkdirAll(dir, materializedDirMode); err != nil {
				return fmt.Errorf("failed to create directory %q: %w", dir, err)
			}

			// Get sorted list of paths to ensure deterministic behavior
			paths := make([]string, 0, len(entry.Paths))
			for p := range entry.Paths {
				paths = append(paths, p)
			}
			sort.Strings(paths)

			// Use first path as source
			src := paths[0]
			ext := filepath.Ext(src)
			if ext == "." {
				ext = ""
			}

			name := fmt.Sprintf("%x%s", hash, ext)
			dst := filepath.Join(dir, name)

			// Check if file already exists
			if _, err := os.Stat(dst); err == nil {
				logger.Debug("skipping existing file", "source", src, "destination", dst)
				continue
			} else if !os.IsNotExist(err) {
				return fmt.Errorf("failed to stat %q: %w", dst, err)
			}

			// Copy file with hash verification
			if err := copyFileWithHash(hash, src, dst, entry.Timestamp); err != nil {
				return fmt.Errorf("failed to copy %q to %q: %w", src, dst, err)
			}

			// Copy attachments
			for attachExt, attachSrc := range entry.Attachments {
				attachName := fmt.Sprintf("%x%s", hash, attachExt)
				attachDst := filepath.Join(dir, attachName)
				if err := copyFile(attachSrc, attachDst); err != nil {
					return fmt.Errorf("failed to copy attachment %q to %q: %w", attachSrc, attachDst, err)
				}
			}
		}

		return nil
	})
}

// copyFile copies a file from src to dst using a temporary file for atomicity.
func copyFile(src, dst string) error {
	if src == "" {
		return errors.New("source path cannot be empty")
	}
	if dst == "" {
		return errors.New("destination path cannot be empty")
	}

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer in.Close()

	tmpFile, err := os.CreateTemp(filepath.Dir(dst), ".venn-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()

	// Ensure cleanup on error
	success := false
	defer func() {
		if !success {
			os.Remove(tmpName)
		}
	}()

	if _, err = io.Copy(tmpFile, in); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to copy data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	success = true
	return nil
}

// copyFileWithHash copies a file from src to dst, verifying the hash matches the expected value.
func copyFileWithHash(hash []byte, src, dst string, timestamp time.Time) error {
	if len(hash) == 0 {
		return errors.New("hash cannot be empty")
	}
	if src == "" {
		return errors.New("source path cannot be empty")
	}
	if dst == "" {
		return errors.New("destination path cannot be empty")
	}

	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer in.Close()

	tmpFile, err := os.CreateTemp(filepath.Dir(dst), ".venn-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()

	// Ensure cleanup on error
	success := false
	defer func() {
		if !success {
			os.Remove(tmpName)
		}
	}()

	// Copy while computing hash
	h := sha256.New()
	teeReader := io.TeeReader(in, h)
	if _, err = io.Copy(tmpFile, teeReader); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to copy data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Verify hash matches - if not, the index is stale
	if !bytes.Equal(hash, h.Sum(nil)) {
		return errors.New("hash mismatch: index is stale")
	}

	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	// Preserve the timestamp from the index
	if err := os.Chtimes(dst, timestamp, timestamp); err != nil {
		return fmt.Errorf("failed to set file times: %w", err)
	}

	success = true
	return nil
}

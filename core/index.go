package core

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	"github.com/ryanuber/columnize"
	bolt "go.etcd.io/bbolt"
)

const (
	// defaultContentType is used when file content type cannot be determined
	defaultContentType = "application/octet-stream"
	// minSizeForContentDetection is the minimum file size needed for content type detection
	minSizeForContentDetection = 512
)

// IndexAddFiles indexes all files in the given root path.
func IndexAddFiles(logger hclog.Logger, indexName, rootPath string) error {
	return indexAdd(logger, indexFile, indexName, rootPath)
}

// IndexAddGooglePhotosTakeout indexes files from a Google Photos takeout, preserving timestamps from metadata.
func IndexAddGooglePhotosTakeout(logger hclog.Logger, indexName, rootPath string) error {
	return indexAdd(logger, indexGooglePhotosTakeout, indexName, rootPath)
}

// indexFn is a function type for indexing a file.
type indexFn func(logger hclog.Logger, b *bolt.Bucket, path string, info os.FileInfo) error

// indexAdd adds files to an index using the provided indexing function.
func indexAdd(logger hclog.Logger, fn indexFn, indexName, rootPath string) error {
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

	count, err := countFiles(logger, rootPath)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	bar := pb.StartNew(count)
	defer bar.Finish()

	return db.Update(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndex(tx, indexName, hashesBucketKey)
		if err != nil {
			return err
		}

		return filepath.Walk(rootPath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return fmt.Errorf("walk error at %q: %w", path, err)
				}

				if info.IsDir() {
					return nil
				}

				if err := fn(logger, bucket, path, info); err != nil {
					return fmt.Errorf("failed to index %q: %w", path, err)
				}

				bar.Increment()
				return nil
			})
	})
}

// countFiles counts the number of files in the given root path.
func countFiles(logger hclog.Logger, rootPath string) (int, error) {
	count := 0
	err := filepath.Walk(rootPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("walk error at %q: %w", path, err)
			}

			if info.IsDir() {
				return nil
			}

			count++
			return nil
		})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// makeFileEntry creates an index entry for a file, computing its hash and metadata.
func makeFileEntry(logger hclog.Logger, bucket *bolt.Bucket, path string, info os.FileInfo) ([]byte, *indexEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Compute SHA-256 hash
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, nil, fmt.Errorf("failed to hash file: %w", err)
	}
	hash := h.Sum(nil)

	// Check if entry already exists
	entry, err := getEntry(bucket, hash)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get existing entry: %w", err)
	}

	// Create new entry if it doesn't exist
	if entry == nil {
		contentType, err := detectContentType(logger, f, info)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to detect content type: %w", err)
		}

		entry = &indexEntry{
			Paths:       make(map[string]struct{}),
			Attachments: make(map[string]string),
			Size:        info.Size(),
			Timestamp:   info.ModTime(),
			ContentType: contentType,
		}
	}

	// Add this path to the entry
	entry.Paths[path] = struct{}{}
	return hash, entry, nil
}

// detectContentType detects the MIME type of a file.
func detectContentType(logger hclog.Logger, f *os.File, info os.FileInfo) (string, error) {
	// Only try if there's enough data to classify, otherwise we may get EOF errors
	if info.Size() < minSizeForContentDetection {
		return defaultContentType, nil
	}

	if _, err := f.Seek(0, 0); err != nil {
		return "", fmt.Errorf("failed to seek to file start: %w", err)
	}

	head := make([]byte, minSizeForContentDetection)
	if _, err := f.Read(head); err != nil {
		return "", fmt.Errorf("failed to read file header: %w", err)
	}

	contentType := http.DetectContentType(head)
	// Remove charset information (e.g., "text/plain; charset=utf-8" -> "text/plain")
	if idx := strings.IndexByte(contentType, ';'); idx != -1 {
		contentType = contentType[:idx]
	}

	return contentType, nil
}

// indexFile indexes a regular file.
func indexFile(logger hclog.Logger, bucket *bolt.Bucket, path string, info os.FileInfo) error {
	hash, entry, err := makeFileEntry(logger, bucket, path, info)
	if err != nil {
		return err
	}
	return putEntry(bucket, hash, entry)
}

// indexGooglePhotosTakeout indexes a file from Google Photos takeout, handling metadata files.
func indexGooglePhotosTakeout(logger hclog.Logger, bucket *bolt.Bucket, path string, info os.FileInfo) error {
	const metadataExt = ".json"

	// Skip metadata files that have a companion content file
	// (they will be processed with the main file)
	if strings.HasSuffix(path, metadataExt) {
		base := strings.TrimSuffix(path, metadataExt)
		if _, err := os.Stat(base); err == nil {
			logger.Debug("skipping metadata file with companion", "path", path, "companion", base)
			return nil
		}
	}

	hash, entry, err := makeFileEntry(logger, bucket, path, info)
	if err != nil {
		return err
	}

	// Check for metadata file and extract timestamp
	metadataPath := path + metadataExt
	if _, err := os.Stat(metadataPath); err == nil {
		timestamp, err := getTakeoutTimestamp(metadataPath)
		if err != nil {
			logger.Warn("failed to extract timestamp from metadata", "metadata", metadataPath, "error", err)
		} else {
			entry.Timestamp = timestamp
			entry.Attachments[metadataExt] = metadataPath
		}
	}

	return putEntry(bucket, hash, entry)
}

// IndexCat displays the contents of an index in a table format.
func IndexCat(logger hclog.Logger, indexName string) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
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

		rows := []string{"SHA-256|Bytes|Timestamp|Content Type|Path(s)"}
		cursor := bucket.Cursor()
		for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
			entry, err := decodeEntry(entryData)
			if err != nil {
				return fmt.Errorf("failed to decode entry: %w", err)
			}

			// Get sorted list of paths for consistent output
			paths := make([]string, 0, len(entry.Paths))
			for p := range entry.Paths {
				paths = append(paths, p)
			}
			sort.Strings(paths)

			row := fmt.Sprintf("%x|%d|%s|%s|%s",
				hash, entry.Size, entry.Timestamp.Format(time.RFC3339),
				entry.ContentType, strings.Join(paths, ","))
			rows = append(rows, row)
		}

		fmt.Println(columnize.SimpleFormat(rows))
		return nil
	})
}

// IndexChunk splits an index into multiple smaller indexes.
func IndexChunk(logger hclog.Logger, indexName, targetIndexPrefix string, chunkSize int) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
	}
	if targetIndexPrefix == "" {
		return errors.New("target index prefix cannot be empty")
	}
	if chunkSize <= 0 {
		return errors.New("chunk size must be positive")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	count := 0
	chunkNum := 0

	return db.Update(func(tx *bolt.Tx) error {
		sourceBucket, err := getBucketForIndex(tx, indexName, hashesBucketKey)
		if err != nil {
			return err
		}

		targetBucket, err := getBucketForIndex(tx, fmt.Sprintf("%s-%d", targetIndexPrefix, chunkNum), hashesBucketKey)
		if err != nil {
			return err
		}

		cursor := sourceBucket.Cursor()
		for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
			if err = targetBucket.Put(hash, entryData); err != nil {
				return fmt.Errorf("failed to put entry in chunk %d: %w", chunkNum, err)
			}

			count++
			if count%chunkSize == 0 {
				chunkNum++
				targetBucket, err = getBucketForIndex(tx, fmt.Sprintf("%s-%d", targetIndexPrefix, chunkNum), hashesBucketKey)
				if err != nil {
					return err
				}
			}
		}

		logger.Info("index chunked successfully", "source", indexName, "chunks", chunkNum+1, "entries", count)
		return nil
	})
}

// IndexList lists all indexes in the database.
func IndexList(logger hclog.Logger) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		bucket, err := getBucketForIndexes(tx)
		if err != nil {
			return err
		}

		cursor := bucket.Cursor()
		for indexName, _ := cursor.First(); indexName != nil; indexName, _ = cursor.Next() {
			fmt.Println(string(indexName))
		}
		return nil
	})
}

// IndexStats displays statistics about an index.
func IndexStats(logger hclog.Logger, indexName string) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
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

		var (
			hashCount     int
			totalBytes    int64
			fileCount     int
			duplicateHash int
			contentTypes  = make(map[string]int)
		)

		cursor := bucket.Cursor()
		for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
			entry, err := decodeEntry(entryData)
			if err != nil {
				return fmt.Errorf("failed to decode entry: %w", err)
			}

			hashCount++
			totalBytes += entry.Size
			fileCount += len(entry.Paths)

			if len(entry.Paths) > 1 {
				duplicateHash++
			}

			contentTypes[entry.ContentType]++
		}

		// Display content type distribution
		rows := []string{"File Type|Hash Count"}
		for contentType, count := range contentTypes {
			rows = append(rows, fmt.Sprintf("%s|%d", contentType, count))
		}
		sort.Strings(rows[1:]) // Sort all but the header
		fmt.Println(columnize.SimpleFormat(rows))
		fmt.Println()

		// Display summary
		fmt.Printf("%d hashes for %d files (%d hashes with duplicates); %d bytes total\n",
			hashCount, fileCount, duplicateHash, totalBytes)
		return nil
	})
}

// IndexDelete deletes an index from the database.
func IndexDelete(logger hclog.Logger, indexName string) error {
	if indexName == "" {
		return errors.New("index name cannot be empty")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if err := deleteBucketForIndex(tx, indexName); err != nil {
			return err
		}
		logger.Info("index deleted successfully", "index", indexName)
		return nil
	})
}

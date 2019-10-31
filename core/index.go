package core

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	"github.com/ryanuber/columnize"
	bolt "go.etcd.io/bbolt"
)

func bucketForIndex(indexName string) []byte {
	return []byte(fmt.Sprintf("index:%s", indexName))
}

func IndexAdd(logger hclog.Logger, dbPath, indexName, rootPath string) error {
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketForIndex(indexName))
		if err != nil {
			return err
		}

		return indexPath(logger, b, rootPath)
	})
}

func indexPath(logger hclog.Logger, b *bolt.Bucket, rootPath string) error {
	count := 0
	err := filepath.Walk(rootPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			count++
			return nil
		})
	if err != nil {
		return err
	}

	bar := pb.StartNew(count)
	defer bar.Finish()
	return filepath.Walk(rootPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("Failed to open %q: %v", path, err)
			}
			defer f.Close()

			// Calculate the hash of the file.
			h := sha256.New()
			if _, err := io.Copy(h, f); err != nil {
				return fmt.Errorf("Failed to hash %q: %v", path, err)
			}
			hash := h.Sum(nil)

			entry, err := getEntry(b, hash)
			if err != nil {
				return err
			}
			if entry == nil {
				// Grab the first part of the file and try to determine its mime type.
				if _, err := f.Seek(0, 0); err != nil {
					return fmt.Errorf("Failed to seek %q: %v", path, err)
				}

				// Only try if there's enough in there to classify, otherwise we will
				// get EOF errors.
				contentType := "application/octet-stream"
				if info.Size() >= 512 {
					head := make([]byte, 512)
					if _, err := f.Read(head); err != nil {
						return fmt.Errorf("Failed to scan %q: %v", path, err)
					}
					contentType = http.DetectContentType(head)
					contentType = strings.Split(contentType, ";")[0]
				}

				entry = &indexEntry{
					Paths:       make(map[string]struct{}),
					Size:        info.Size(),
					ContentType: contentType,
				}
			}
			entry.Paths[path] = struct{}{}
			if err := putEntry(b, hash, entry); err != nil {
				return err
			}

			bar.Increment()
			return nil
		})
}

func IndexList(logger hclog.Logger, dbPath, indexName string) error {
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketForIndex(indexName))
		if b == nil {
			return fmt.Errorf("Index %q does not exist", indexName)
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}

			var paths []string
			for p := range entry.Paths {
				paths = append(paths, p)
			}
			sort.Strings(paths)

			fmt.Printf("%x %10d %25s %v\n", k, entry.Size, entry.ContentType, strings.Join(paths, ","))
		}
		return nil
	})
}

func IndexStats(logger hclog.Logger, dbPath, indexName string) error {
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		hashes := 0
		var bytes int64
		files := 0
		dups := 0
		types := make(map[string]int)

		b := tx.Bucket(bucketForIndex(indexName))
		if b == nil {
			return fmt.Errorf("Index %q does not exist", indexName)
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}

			hashes++
			bytes += entry.Size
			files += len(entry.Paths)
			if len(entry.Paths) > 1 {
				dups++
			}
			if _, ok := types[entry.ContentType]; !ok {
				types[entry.ContentType] = 0
			}
			types[entry.ContentType]++
		}

		var rows []string
		for t, c := range types {
			rows = append(rows, fmt.Sprintf("%s|%d", t, c))
		}
		sort.Strings(rows)
		rows = append([]string{"File Type|Hash Count"}, rows...)
		fmt.Println(columnize.SimpleFormat(rows))
		fmt.Println("")
		fmt.Printf("%d hashes for %d files (%d hashes with duplicates); %d bytes total\n", hashes, files, dups, bytes)
		return nil
	})
}

func IndexDelete(logger hclog.Logger, dbPath, indexName string) error {
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketForIndex(indexName))
		if b == nil {
			return fmt.Errorf("Index %q does not exist", indexName)
		}

		return tx.DeleteBucket(bucketForIndex(indexName))
	})
}

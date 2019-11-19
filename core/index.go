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
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	"github.com/ryanuber/columnize"
	bolt "go.etcd.io/bbolt"
)

func IndexAddFiles(logger hclog.Logger, indexName, rootPath string) error {
	return indexAdd(logger, indexFile, indexName, rootPath)
}

func IndexAddGooglePhotosTakeout(logger hclog.Logger, indexName, rootPath string) error {
	return indexAdd(logger, indexGooglePhotosTakeout, indexName, rootPath)
}

type indexFn func(logger hclog.Logger, b *bolt.Bucket, path string, info os.FileInfo) error

func indexAdd(logger hclog.Logger, fn indexFn, indexName, rootPath string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	count, err := countFiles(logger, rootPath)
	if err != nil {
		return err
	}
	bar := pb.StartNew(count)
	defer bar.Finish()

	return db.Update(func(tx *bolt.Tx) error {
		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
		}

		return filepath.Walk(rootPath,
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if info.IsDir() {
					return nil
				}

				if err := fn(logger, b, path, info); err != nil {
					return fmt.Errorf("Failed to index %q: %v", path, err)
				}

				bar.Increment()
				return nil
			})
	})
}

func countFiles(logger hclog.Logger, rootPath string) (int, error) {
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
	return count, err
}

func makeFileEntry(logger hclog.Logger, b *bolt.Bucket, path string, info os.FileInfo) ([]byte, *indexEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, nil, err
	}
	hash := h.Sum(nil)

	entry, err := getEntry(b, hash)
	if err != nil {
		return nil, nil, err
	}
	if entry == nil {
		contentType, err := grokFile(logger, f, info)
		if err != nil {
			return nil, nil, err
		}

		entry = &indexEntry{
			Paths:       make(map[string]struct{}),
			Attachments: make(map[string]string),
			Size:        info.Size(),
			Timestamp:   info.ModTime(),
			ContentType: contentType,
		}
	}
	entry.Paths[path] = struct{}{}
	return hash, entry, nil
}

func grokFile(logger hclog.Logger, f *os.File, info os.FileInfo) (string, error) {
	// Only try if there's enough in there to classify, otherwise we will
	// get EOF errors.
	contentType := "application/octet-stream"
	if info.Size() < 512 {
		return contentType, nil
	}

	if _, err := f.Seek(0, 0); err != nil {
		return "", err
	}
	head := make([]byte, 512)
	if _, err := f.Read(head); err != nil {
		return "", err
	}
	contentType = http.DetectContentType(head)
	contentType = strings.Split(contentType, ";")[0]
	return contentType, nil
}

func indexFile(logger hclog.Logger, b *bolt.Bucket, path string, info os.FileInfo) error {
	hash, entry, err := makeFileEntry(logger, b, path, info)
	if err != nil {
		return err
	}
	return putEntry(b, hash, entry)
}

func indexGooglePhotosTakeout(logger hclog.Logger, b *bolt.Bucket, path string, info os.FileInfo) error {
	// Skip any file with a .json extension that has a companion file with the same path
	// but without that extension; that's a metatdata file that we will process with the
	// main file.
	const ext = ".json"
	if strings.HasSuffix(path, ext) {
		base := strings.TrimSuffix(path, ext)
		if _, err := os.Stat(base); err == nil {
			return nil
		}
	}

	hash, entry, err := makeFileEntry(logger, b, path, info)
	if err != nil {
		return err
	}

	metadata := path + ext
	if _, err := os.Stat(metadata); err == nil {
		ts, err := getTakeoutTimestamp(metadata)
		if err != nil {
			return err
		}

		entry.Timestamp = ts
		entry.Attachments[ext] = metadata
	}

	return putEntry(b, hash, entry)
}

func IndexCat(logger hclog.Logger, indexName string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
		}

		var rows []string
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

			rows = append(rows,
				fmt.Sprintf("%x|%d|%s|%s|%v\n",
					k, entry.Size, entry.Timestamp.Format(time.RFC3339),
					entry.ContentType, strings.Join(paths, ",")))
		}

		rows = append([]string{"SHA-256|Bytes|Timestamp|Content Type|Path(s)"}, rows...)
		fmt.Println(columnize.SimpleFormat(rows))
		return nil
	})
}

func IndexList(logger hclog.Logger) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(tx *bolt.Tx) error {
		b, err := getBucketForIndexes(tx)
		if err != nil {
			return err
		}

		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			fmt.Println(string(k))
		}
		return nil
	})
}

func IndexStats(logger hclog.Logger, indexName string) error {
	db, err := getDB()
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

		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
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

func IndexDelete(logger hclog.Logger, indexName string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		return deleteBucketForIndex(tx, indexName)
	})
}

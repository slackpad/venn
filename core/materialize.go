package core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func Materialize(logger hclog.Logger, indexName, rootPath string) error {
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

		bar := pb.StartNew(b.Stats().KeyN)
		c := b.Cursor()
	FILES:
		for k, v := c.First(); k != nil; k, v = c.Next() {
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}

			f0 := fmt.Sprintf("%02x", k[0])
			f1 := fmt.Sprintf("%02x", k[1])
			dir := filepath.Join(rootPath, f0, f1)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return err
			}

			var paths []string
			for p := range entry.Paths {
				paths = append(paths, p)
			}
			sort.Strings(paths)

			src := paths[0]
			ext := filepath.Base(filepath.Ext(src))
			if ext == "." {
				ext = ""
			}

			name := fmt.Sprintf("%x%s", k, ext)
			dst := filepath.Join(dir, name)
			_, err = os.Stat(dst)
			if os.IsNotExist(err) {
				// Fall through, we need to copy the file.
			} else if err != nil {
				return err
			} else {
				logger.Debug("Skipping copy of existing file", "path", src)
				continue FILES
			}

			if err := copyFileWithHash(k, src, dst, entry.Timestamp); err != nil {
				return fmt.Errorf("Failed to copy %q: %v", src, err)
			}

			if len(paths) > 1 {
				dir = filepath.Join(rootPath, "_dups", f0, f1, fmt.Sprintf("%x", k))
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				for i, src := range paths {
					name = fmt.Sprintf("%d%s", i, ext)
					dst = filepath.Join(dir, name)
					if err := copyFileWithHash(k, src, dst, entry.Timestamp); err != nil {
						return fmt.Errorf("Failed to copy %q: %v", src, err)
					}
				}
			}

			for ext, src := range entry.Attachments {
				name := fmt.Sprintf("%x%s", k, ext)
				dst := filepath.Join(dir, name)
				if err := copyFile(src, dst); err != nil {
					return fmt.Errorf("Failed to copy %q: %v", src, err)
				}
			}

			bar.Increment()
		}
		bar.Finish()
		return nil
	})
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := ioutil.TempFile(filepath.Dir(dst), "")
	if err != nil {
		return err
	}

	_, err = io.Copy(tmp, in)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return os.Rename(tmp.Name(), dst)
}

func copyFileWithHash(hash []byte, src, dst string, timestamp time.Time) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := ioutil.TempFile(filepath.Dir(dst), "")
	if err != nil {
		return err
	}

	h := sha256.New()
	tee := io.TeeReader(in, h)
	_, err = io.Copy(tmp, tee)
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	// Make sure it landed with the expected hash, otherwise the index
	// is out of date and we will corrupt our content addressable layout.
	if !bytes.Equal(hash, h.Sum(nil)) {
		return fmt.Errorf("Hash does not match, index is stale")
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}

	// Keep the time from the index.
	if err := os.Chtimes(dst, timestamp, timestamp); err != nil {
		return err
	}

	return nil
}

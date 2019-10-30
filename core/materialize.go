package core

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/cheggaaa/pb/v3"
	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func Materialize(logger hclog.Logger, dbPath, indexName, rootPath string) error {
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

		bar := pb.StartNew(b.Stats().KeyN)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			entry, err := decodeEntry(v)
			if err != nil {
				return err
			}

			f0 := fmt.Sprintf("%02x", k[0])
			f1 := fmt.Sprintf("%02x", k[1])
			dir := filepath.Join(rootPath, f0, f1)
			if err := os.MkdirAll(dir, os.ModeDir); err != nil {
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
			if err := copyFile(src, dst); err != nil {
				return fmt.Errorf("Failed to copy %q: %v", src, err)
			}

			if len(paths) > 1 {
				dir = filepath.Join(rootPath, "_dups", f0, f1, fmt.Sprintf("%x", k))
				if err := os.MkdirAll(dir, os.ModeDir); err != nil {
					return err
				}
				for i, src := range paths {
					name = fmt.Sprintf("%d%s", i, ext)
					dst = filepath.Join(dir, name)
					if err := copyFile(src, dst); err != nil {
						return fmt.Errorf("Failed to copy %q: %v", src, err)
					}
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
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return err
	}
	return nil
}

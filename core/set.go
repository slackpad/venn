package core

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func SetDifference(logger hclog.Logger, indexName, indexNameA, indexNameB string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, indexName) {
			return fmt.Errorf("Target index %q already exists", indexName)
		}

		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
		}

		ab, err := getBucketForIndex(tx, indexNameA, "HASHES")
		if err != nil {
			return err
		}

		bb, err := getBucketForIndex(tx, indexNameB, "HASHES")
		if err != nil {
			return err
		}

		c := ab.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bb.Get(k) == nil {
				if err := b.Put(k, v); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func SetIntersection(logger hclog.Logger, indexName, indexNameA, indexNameB string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, indexName) {
			return fmt.Errorf("Target index %q already exists", indexName)
		}

		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
		}

		ab, err := getBucketForIndex(tx, indexNameA, "HASHES")
		if err != nil {
			return err
		}

		bb, err := getBucketForIndex(tx, indexNameB, "HASHES")
		if err != nil {
			return err
		}

		return intersect(ab, bb, b)
	})
}

func SetUnion(logger hclog.Logger, indexName, indexNameA, indexNameB string) error {
	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, indexName) {
			return fmt.Errorf("Target index %q already exists", indexName)
		}

		b, err := getBucketForIndex(tx, indexName, "HASHES")
		if err != nil {
			return err
		}

		ab, err := getBucketForIndex(tx, indexNameA, "HASHES")
		if err != nil {
			return err
		}

		bb, err := getBucketForIndex(tx, indexNameB, "HASHES")
		if err != nil {
			return err
		}

		return merge(ab, bb, b)
	})
}

func merge(first, second, target *bolt.Bucket) error {
	c := first.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		entry, err := decodeEntry(v)
		if err != nil {
			return err
		}

		other, err := getEntry(second, k)
		if err != nil {
			return err
		}
		if other != nil {
			entry.merge(other)
		}

		if err := putEntry(target, k, entry); err != nil {
			return err
		}
	}

	c = second.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		other := first.Get(k)
		if other != nil {
			// We merged it already above.
			continue
		}

		if err := target.Put(k, v); err != nil {
			return err
		}
	}

	return nil
}

func intersect(first, second, target *bolt.Bucket) error {
	c := first.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		entry, err := decodeEntry(v)
		if err != nil {
			return err
		}

		other, err := getEntry(second, k)
		if err != nil {
			return err
		}
		if other != nil {
			entry.merge(other)
			if err := putEntry(target, k, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

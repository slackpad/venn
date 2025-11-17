package core

import (
	"errors"
	"fmt"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

// SetDifference creates a new index containing entries in A but not in B (A - B).
func SetDifference(logger hclog.Logger, targetIndex, indexA, indexB string) error {
	if targetIndex == "" {
		return errors.New("target index name cannot be empty")
	}
	if indexA == "" {
		return errors.New("index A name cannot be empty")
	}
	if indexB == "" {
		return errors.New("index B name cannot be empty")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, targetIndex) {
			return fmt.Errorf("target index %q already exists", targetIndex)
		}

		targetBucket, err := getBucketForIndex(tx, targetIndex, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketA, err := getBucketForIndex(tx, indexA, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketB, err := getBucketForIndex(tx, indexB, hashesBucketKey)
		if err != nil {
			return err
		}

		// Add entries from A that are not in B
		count := 0
		cursor := bucketA.Cursor()
		for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
			if bucketB.Get(hash) == nil {
				if err := targetBucket.Put(hash, entryData); err != nil {
					return fmt.Errorf("failed to put entry: %w", err)
				}
				count++
			}
		}

		logger.Info("set difference completed", "target", targetIndex, "A", indexA, "B", indexB, "entries", count)
		return nil
	})
}

// SetIntersection creates a new index containing entries in both A and B (A ∩ B).
func SetIntersection(logger hclog.Logger, targetIndex, indexA, indexB string) error {
	if targetIndex == "" {
		return errors.New("target index name cannot be empty")
	}
	if indexA == "" {
		return errors.New("index A name cannot be empty")
	}
	if indexB == "" {
		return errors.New("index B name cannot be empty")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, targetIndex) {
			return fmt.Errorf("target index %q already exists", targetIndex)
		}

		targetBucket, err := getBucketForIndex(tx, targetIndex, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketA, err := getBucketForIndex(tx, indexA, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketB, err := getBucketForIndex(tx, indexB, hashesBucketKey)
		if err != nil {
			return err
		}

		count, err := intersect(bucketA, bucketB, targetBucket)
		if err != nil {
			return err
		}

		logger.Info("set intersection completed", "target", targetIndex, "A", indexA, "B", indexB, "entries", count)
		return nil
	})
}

// SetUnion creates a new index containing all entries from A and B (A ∪ B).
func SetUnion(logger hclog.Logger, targetIndex, indexA, indexB string) error {
	if targetIndex == "" {
		return errors.New("target index name cannot be empty")
	}
	if indexA == "" {
		return errors.New("index A name cannot be empty")
	}
	if indexB == "" {
		return errors.New("index B name cannot be empty")
	}

	db, err := getDB()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		if bucketExistsForIndex(tx, targetIndex) {
			return fmt.Errorf("target index %q already exists", targetIndex)
		}

		targetBucket, err := getBucketForIndex(tx, targetIndex, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketA, err := getBucketForIndex(tx, indexA, hashesBucketKey)
		if err != nil {
			return err
		}

		bucketB, err := getBucketForIndex(tx, indexB, hashesBucketKey)
		if err != nil {
			return err
		}

		count, err := merge(bucketA, bucketB, targetBucket)
		if err != nil {
			return err
		}

		logger.Info("set union completed", "target", targetIndex, "A", indexA, "B", indexB, "entries", count)
		return nil
	})
}

// merge combines entries from first and second buckets into the target bucket.
// When a hash exists in both buckets, the entries are merged.
// Returns the number of entries added to the target.
func merge(first, second, target *bolt.Bucket) (int, error) {
	count := 0

	// Process all entries from first bucket
	cursor := first.Cursor()
	for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
		entry, err := decodeEntry(entryData)
		if err != nil {
			return count, fmt.Errorf("failed to decode entry from first bucket: %w", err)
		}

		// Check if this hash exists in second bucket
		otherEntry, err := getEntry(second, hash)
		if err != nil {
			return count, fmt.Errorf("failed to get entry from second bucket: %w", err)
		}

		// Merge if found in both
		if otherEntry != nil {
			entry.merge(otherEntry)
		}

		if err := putEntry(target, hash, entry); err != nil {
			return count, fmt.Errorf("failed to put merged entry: %w", err)
		}
		count++
	}

	// Process entries from second bucket that weren't in first
	cursor = second.Cursor()
	for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
		// Skip if already processed (exists in first)
		if first.Get(hash) != nil {
			continue
		}

		if err := target.Put(hash, entryData); err != nil {
			return count, fmt.Errorf("failed to put entry from second bucket: %w", err)
		}
		count++
	}

	return count, nil
}

// intersect creates entries in the target bucket for hashes that exist in both first and second.
// Entries from both buckets are merged.
// Returns the number of entries added to the target.
func intersect(first, second, target *bolt.Bucket) (int, error) {
	count := 0

	cursor := first.Cursor()
	for hash, entryData := cursor.First(); hash != nil; hash, entryData = cursor.Next() {
		entry, err := decodeEntry(entryData)
		if err != nil {
			return count, fmt.Errorf("failed to decode entry from first bucket: %w", err)
		}

		// Check if this hash exists in second bucket
		otherEntry, err := getEntry(second, hash)
		if err != nil {
			return count, fmt.Errorf("failed to get entry from second bucket: %w", err)
		}

		// Only include if found in both buckets
		if otherEntry != nil {
			entry.merge(otherEntry)
			if err := putEntry(target, hash, entry); err != nil {
				return count, fmt.Errorf("failed to put intersected entry: %w", err)
			}
			count++
		}
	}

	return count, nil
}

package core

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	bolt "go.etcd.io/bbolt"
)

func SetDifference(logger hclog.Logger, dbPath, indexName, indexNameA, indexNameB string) error {
	db, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		out := bucketForIndex(indexName)
		b := tx.Bucket(out)
		if b != nil {
			return fmt.Errorf("Index %q already exists", indexName)
		}
		b, err = tx.CreateBucket(out)
		if err != nil {
			return err
		}

		ab := tx.Bucket(bucketForIndex(indexNameA))
		if ab == nil {
			return fmt.Errorf("Index %q does not exist", indexNameA)
		}

		bb := tx.Bucket(bucketForIndex(indexNameB))
		if bb == nil {
			return fmt.Errorf("Index %q does not exist", indexNameB)
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

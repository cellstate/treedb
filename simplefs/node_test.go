package simplefs

import (
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func TestCreateEmptyDirNode(t *testing.T) {
	db, close := testdb(t)
	defer close()

	var n *node
	var id uint64
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(FileBucketName)
		if err != nil {
			return err
		}

		ntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		id, n, err = ntx.putNode(os.ModeDir | 0777)
		return err
	}); err != nil {
		t.Error(err)
	}

	if id != 1 {
		t.Errorf("unexpected node id, got: %v", id)
	}

	fi := &fileInfo{node: n, name: "foo"}
	if fi.ModTime().IsZero() {
		t.Error("node fi shouldnt have empty time")
	}

	if !fi.IsDir() {
		t.Error("node fi should be directory")
	}

	if fi.Mode().Perm() != 0777 {
		t.Errorf("node fi perm should be this, got: %s", fi.Mode().Perm())
	}
}

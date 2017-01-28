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

func TestCreateEmptyFileInDirNode(t *testing.T) {
	db, close := testdb(t)
	defer close()

	var fN *node
	var fID uint64
	var dN *node
	var dID uint64
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(FileBucketName)
		if err != nil {
			return err
		}

		fntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		fID, fN, err = fntx.putNode(0777)
		if err != nil {
			return err
		}

		dntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		err = dntx.putChildPtr("foo.txt", fID)
		if err != nil {
			return err
		}

		dID, dN, err = dntx.putNode(os.ModeDir | 0777)
		return err
	}); err != nil {
		t.Error(err)
	}

	var d2N *node
	children := map[string]uint64{}
	if err := db.View(func(tx *bolt.Tx) error {
		dntx, err := newNodeTx(tx, dID)
		if err != nil {
			return err
		}

		d2N, err = dntx.getNode()
		if err != nil {
			return err
		}

		if err = dntx.getChildPtrs(func(n string, id uint64) error {
			children[n] = id
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Error(err)
	}

	fi := &fileInfo{node: d2N, name: "foo"}
	if fi.ModTime().IsZero() {
		t.Error("node fi shouldnt have empty time")
	}

	if !fi.IsDir() {
		t.Error("node fi should be directory")
	}

	if fi.Mode().Perm() != 0777 {
		t.Errorf("node fi perm should be this, got: %s", fi.Mode().Perm())
	}

	if len(children) != 1 {
		t.Error("expected one child node")
	}

	if c, ok := children["foo.txt"]; ok {
		if c != 1 {
			t.Error("expected child id to be this exactly")
		}
	} else {
		t.Errorf("expected correct child nodes, got: %+v", children)
	}
}

package simplefs

import (
	"crypto/rand"
	"fmt"
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

func TestDescendInDirNodes(t *testing.T) {
	db, close := testdb(t)
	defer close()

	var root uint64
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(FileBucketName)
		if err != nil {
			return err
		}

		// /*/*/<f1id>
		var f1id uint64
		f1ntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		f1id, _, err = f1ntx.putNode(os.ModeDir | 0777)
		if err != nil {
			return err
		}

		// /*/<f2id>/<f1id>
		var f2id uint64
		f2ntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		err = f2ntx.putChildPtr("foo", f1id)
		if err != nil {
			return err
		}

		f2id, _, err = f2ntx.putNode(os.ModeDir | 0777)
		if err != nil {
			return err
		}

		// /<root>/<f2id>/<f1id>
		f3ntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		err = f3ntx.putChildPtr("bar", f2id)
		if err != nil {
			return err
		}

		root, _, err = f3ntx.putNode(os.ModeDir | 0777)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Error(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		ntx, err := newNodeTx(tx, root)
		if err != nil {
			return err
		}

		leafid := ntx.getDescendantID(P{"bogus", "foo"})
		if leafid != 0 {
			return fmt.Errorf("expected descendant not to be found")
		}

		leafid = ntx.getDescendantID(P{"bar", "foo"})
		if leafid != 1 {
			return fmt.Errorf("expected descendant to be this id")
		}

		return nil
	}); err != nil {
		t.Error(err)
	}

	_ = root

}

func TestPutChunkPtrs(t *testing.T) {
	db, close := testdb(t)
	defer close()

	k1 := K{}
	_, err := rand.Read(k1[:])
	if err != nil {
		t.Fatal(err)
	}

	var fid uint64
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(FileBucketName)
		if err != nil {
			return err
		}

		fntx, err := newNodeTx(tx, 0) //new node
		if err != nil {
			return err
		}

		err = fntx.putChunkPtr(0, k1)
		if err != nil {
			return err
		}

		err = fntx.putChunkPtr(4*1024*1024, ZeroKey) //indicate EOF
		if err != nil {
			return err
		}

		fid, _, err = fntx.putNode(os.ModeDir | 0777)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Error(err)
	}

	chunks := map[int64]K{}
	if err := db.View(func(tx *bolt.Tx) error {
		fntx, err := newNodeTx(tx, fid) //new node
		if err != nil {
			return err
		}

		return fntx.getChunkPtrs(func(offset int64, k K) error {
			chunks[offset] = k
			return nil
		})
	}); err != nil {
		t.Error(err)
	}

	if len(chunks) != 2 {
		t.Errorf("expected this number of chunks, got: %+v", chunks)
	}

	if k, ok := chunks[0]; ok {
		if k != k1 {
			t.Error("expected this chunk for offset")
		}
	} else {
		t.Errorf("expected this chunk, got: %+v", chunks)
	}

	if k, ok := chunks[4*1024*1024]; ok {
		if k != ZeroKey {
			t.Error("expected EOF chunk")
		}
	} else {
		t.Errorf("expected this chunk, got: %+v", chunks)
	}
}

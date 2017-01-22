package layerfs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
)

func testdb(t *testing.T) (db *bolt.DB, close func()) {
	tmpdir, err := ioutil.TempDir("", "dfs_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, err = bolt.Open(filepath.Join(tmpdir, "fs.bolt"), 0666, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db: %v", err)
	}

	return db, func() {
		os.RemoveAll(tmpdir)
		db.Close()
	}
}

func testfs(t *testing.T) (fs *LayerFS, close func()) {
	db, close := testdb(t)
	fs, err := New(ZeroKey, db)
	if err != nil {
		t.Fatalf("failed to setup fs: %v", err)
	}

	return fs, close
}

func TestWriteNewBranch(t *testing.T) {
	fs, close := testfs(t)
	defer close()

	n := &Node{
		N: "/",
	}

	var err error
	if err = fs.db.Update(func(tx *bolt.Tx) error {
		b1, err := NewBranchWriter(nil, tx, map[string][]byte{
			"c.txt": []byte("333"),
			"a.txt": []byte("1"),
			"b.txt": []byte("22"),
		})

		_ = b1
		if err != nil {
			return err
		}

		return b1.Commit(tx, n)
	}); err != nil {
		t.Error(err)
	}

	if n.ModTime().IsZero() {
		t.Error("commit should have set modtime")
	}

	if n.Size() < 1 {
		t.Error("commit should have set size")
	}

}

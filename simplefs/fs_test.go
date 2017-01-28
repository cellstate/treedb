package simplefs

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

func testfs(t *testing.T) (fs *FileSystem, close func()) {
	db, close := testdb(t)
	fs, err := New(db)
	if err != nil {
		t.Fatalf("failed to setup fs: %v", err)
	}

	return fs, close
}

func TestStatRoot(t *testing.T) {
	fs, close := testfs(t)
	defer close()

	fi, err := fs.Stat(Root)
	if err != nil {
		t.Fatalf("didn't expect error, got: %v", err)
	}

	if fi.Name() != RootBasename {
		t.Errorf("expected fi name to be root basename, got: %v", fi.Name())
	}

	if !fi.IsDir() {
		t.Errorf("expected root node to be a directory, got: %+v", fi)
	}
}

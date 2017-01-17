package treedb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/boltdb/bolt"
)

// func CaseStatNonExisting(fs *FileSystem, t *testing.T) {
// 	_, err := fs.Stat(P{"foo.txt"})
// 	if !os.IsNotExist(err) {
// 		t.Error("expected non existing file error")
// 	}
// }
//
// func CaseCreateNonExisting(fs *FileSystem, t *testing.T) {
// 	// f, err := fs.Create("foo.txt")
// 	// if err != nil {
// 	// 	t.Fatalf("expected no error, got: %v", err)
// 	// }
// 	//
// 	// if f == nil {
// 	// 	t.Fatal("expected the returned file not to be nil")
// 	// }
// 	//
// 	// _, err = fs.Create("bar/foo.txt")
// 	// if !os.IsNotExist(err) {
// 	// 	t.Error("expected non existing file error")
// 	// }
// 	//
// 	// fi, err := fs.Stat("foo.txt")
// 	// if err != nil {
// 	// 	t.Fatalf("didnt expect stat error, got: %v", err)
// 	// }
// 	//
// 	// if fi.Name() != "foo.txt" {
// 	// 	t.Errorf("base name of file info should be correct, got: '%v'", fi.Name())
// 	// }
// 	//
// 	// if fi.Mode().IsRegular() != true {
// 	// 	t.Error("file info mode should indicate it to be a regular file")
// 	// }
// }
//
// func CaseMkDirNonExisting(fs *FileSystem, t *testing.T) {
// 	err := fs.Mkdir("bar")
// 	if err != nil {
// 		t.Fatalf("expected no error, got: %v", err)
// 	}
// }

func TestAtomicCases(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "dfs_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	defer os.RemoveAll(tmpdir)

	db, err := bolt.Open(filepath.Join(tmpdir, "fs.bolt"), 0666, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db: %v", err)
	}

	defer db.Close()

	cases := []struct {
		Name string
		Case func(fs *FileSystem, t *testing.T)
	}{
	// {Name: "StatNonExisting", Case: CaseStatNonExisting},
	// {Name: "CreateNonExisting", Case: CaseCreateNonExisting},
	// {Name: "MkDirNoNonExisting", Case: CaseMkDirNonExisting},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			fs, err := NewFileSystem(t.Name(), db)
			if err != nil {
				t.Fatal(err)
			}

			c.Case(fs, t)
		})
	}
}

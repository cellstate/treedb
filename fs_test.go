package treedb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
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
	fs, err := NewFileSystem(t.Name(), db)
	if err != nil {
		t.Fatalf("failed to setup fs: %v", err)
	}

	return fs, close
}

func TestGetFileInfoNonExisting(t *testing.T) {
	fs, close := testfs(t)
	defer close()

	err := fs.db.View(func(tx *bolt.Tx) error {
		_, err := fs.getfi(tx, P{"foo.txt"})
		return err
	})

	if err != os.ErrNotExist {
		t.Error("expected error to be os.ErrNotExist")
	}
}

func TestPutFileInfo(t *testing.T) {
	fs, close := testfs(t)
	defer close()

	fiA := &fileInfo{N: "foo.txt"}
	err := fs.db.Update(func(tx *bolt.Tx) error {
		return fs.putfi(tx, P{"foo.txt"}, fiA)
	})

	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	var fiB *fileInfo
	err = fs.db.View(func(tx *bolt.Tx) error {
		fiB, err = fs.getfi(tx, P{"foo.txt"})
		return err
	})

	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if !reflect.DeepEqual(fiA, fiB) {
		t.Errorf("expected read file info to equal written file info")
	}
}

func TestWriteable(t *testing.T) {
	fs, close := testfs(t)
	defer close()

	if fs.mightwrite(os.O_CREATE) == false {
		t.Fatal("expected O_CREATE to be flagged as might write")
	}

	if fs.mightwrite(os.O_RDONLY) == true {
		t.Fatal("expected O_RDONLY to not be flagged as might write")
	}
}

func CaseStatNonExisting(fs *FileSystem, t *testing.T) {
	_, err := fs.Stat(P{"foo.txt"})
	if !os.IsNotExist(err) {
		t.Error("expected non existing file error")
	}
}

func CaseStatInvalidPath(fs *FileSystem, t *testing.T) {
	_, err := fs.Stat(P{"fo/o.txt"})
	if err == nil {
		t.Fatal("expected err")
	}
	perr, ok := err.(*os.PathError)
	if !ok || perr.Err != ErrInvalidPath {
		t.Errorf("expected invalid path err, got: %v", err)
	}
}

func CaseStatExisting(fs *FileSystem, t *testing.T) {
	fi, err := fs.Stat(Root)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if fi.IsDir() != true {
		t.Error("expected root to be a directory")
	}

	if fi.Name() != "/" {
		t.Error("expected basename of root to be /")
	}
}

func CaseOpenFileInvalidPath(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"fo/o.txt"}, 0, 0)
	if err == nil {
		t.Fatal("expected err")
	}
	perr, ok := err.(*os.PathError)
	if !ok || perr.Err != ErrInvalidPath {
		t.Errorf("expected invalid path err, got: %v", err)
	}
}

func CaseOpenFileCreateNonExisting(fs *FileSystem, t *testing.T) {
	f, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if f == nil {
		t.Fatal("opened file should not be nil")
	}

	fi, err := fs.Stat(P{"foo.txt"})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if fi.Name() != "foo.txt" {
		t.Error("newly created basename should be correct")
	}

	if fi.Mode() != 0777 {
		t.Error("expected correct file mode to be set")
	}
}

func CaseMkdirNonExisting(fs *FileSystem, t *testing.T) {
	err := fs.Mkdir(P{"bar"}, 777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	fi, err := fs.Stat(P{"bar"})
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if fi.Mode() != os.ModeDir|777 {
		t.Errorf("expected mode to be set correctly, got: %v", fi.Mode())
	}

	err = fs.Mkdir(P{"bar"}, 777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestAtomicCases(t *testing.T) {
	db, close := testdb(t)
	defer close()

	cases := []struct {
		Name string
		Case func(fs *FileSystem, t *testing.T)
	}{
		{Name: "StatNonExisting", Case: CaseStatNonExisting},
		{Name: "StatNonExisting", Case: CaseStatExisting},
		{Name: "StatNonExisting", Case: CaseStatInvalidPath},

		{Name: "CaseOpenFileInvalidPath", Case: CaseOpenFileInvalidPath},
		{Name: "CaseOpenFileCreateNonExisting", Case: CaseOpenFileCreateNonExisting},

		{Name: "CaseMkdirNonExisting", Case: CaseMkdirNonExisting},
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

package treedb

import (
	"io"
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

func testfiles(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"a.txt"}, os.O_CREATE, 0777)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fs.OpenFile(P{"b.txt"}, os.O_CREATE, 0777)
	if err != nil {
		t.Fatal(err)
	}

	//the one-to last unicode char is still valid and should be ordered before the next dir
	_, err = fs.OpenFile(P{"bar\uFFFEc.txt"}, os.O_CREATE, 0777)
	if err != nil {
		t.Fatal(err)
	}

	err = fs.Mkdir(P{"bar"}, 0777)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fs.OpenFile(P{"bar", "c.txt"}, os.O_CREATE, 0777)
	if err != nil {
		t.Fatal(err)
	}

	return
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
	_, err := fs.Stat(P{"fo\uFFFFo.txt"})
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

	if fi.Name() != PathSeparator {
		t.Error("expected basename of root to be PathSeperator")
	}

	if fi.ModTime().IsZero() {
		t.Error("modtime should not be zero")
	}
}

func CaseOpenFileInvalidPath(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"fo\uFFFFo.txt"}, 0, 0)
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

	if fi.ModTime().IsZero() {
		t.Error("mod time should not be zero")
	}
}

func CaseOpenFileParentNotDirectory(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	_, err = fs.OpenFile(P{"foo.txt", "foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err == nil {
		t.Fatalf("expected error, got: %v", err)
	}

	perr := err.(*os.PathError)
	if perr.Err != ErrNotDirectory {
		t.Fatalf("expected ErrNotDirectory, got: %v", err)
	}
}

func CaseOpenFileParentNotExist(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"bar", "foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err == nil {
		t.Fatalf("expected error, got: %v", err)
	}

	perr := err.(*os.PathError)
	if perr.Err != os.ErrNotExist {
		t.Fatalf("expected ErrNotDirectory, got: %v", err)
	}
}

func CaseOpenFileExclusive(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	_, err = fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0777)
	if err == nil {
		t.Fatalf("expected error, got: %v", err)
	}

	perr := err.(*os.PathError)
	if perr.Err != os.ErrExist {
		t.Fatalf("expected os.ErrExists, got: %v", err)
	}
}

func CaseOpenFileReadOnly(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(Root, os.O_RDONLY, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func CaseOpenFileNonExisting(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"foo.txt"}, os.O_RDWR, 0777)
	if err == os.ErrNotExist {
		t.Fatalf("expected os.ErrNotExist, got: %v", err)
	}
}

func CaseMkdirInvalidPath(fs *FileSystem, t *testing.T) {
	err := fs.Mkdir(P{"fo\uFFFFo.txt"}, 0)
	if err == nil {
		t.Fatal("expected err")
	}
	perr, ok := err.(*os.PathError)
	if !ok || perr.Err != ErrInvalidPath {
		t.Errorf("expected invalid path err, got: %v", err)
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

	if fi.ModTime().IsZero() {
		t.Error("modtime should not be zero")
	}

	err = fs.Mkdir(P{"bar"}, 777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func CaseMkdirExistingFile(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	err = fs.Mkdir(P{"foo.txt"}, 777)
	if err == nil {
		t.Fatalf("expected err")
	}

	perr := err.(*os.PathError)
	if perr.Err != os.ErrExist {
		t.Fatalf("expected os.ErrExists, got: %v", err)
	}
}

func CaseMkdirParentNotExist(fs *FileSystem, t *testing.T) {
	err := fs.Mkdir(P{"foo", "bar"}, 777)
	if err == nil {
		t.Fatalf("expected err")
	}

	perr := err.(*os.PathError)
	if perr.Err != os.ErrNotExist {
		t.Fatalf("expected os.ErrNotExist, got: %v", err)
	}
}

func CaseMkdirParentNotDirectory(fs *FileSystem, t *testing.T) {
	_, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	err = fs.Mkdir(P{"foo.txt", "foo"}, 777)
	if err == nil {
		t.Fatalf("expected err")
	}

	perr := err.(*os.PathError)
	if perr.Err != ErrNotDirectory {
		t.Fatalf("expected ErrNotDirectory, got: %v", err)
	}
}

func CaseFileReaddirAll(fs *FileSystem, t *testing.T) {
	testfiles(fs, t)

	f, err := fs.Open(Root)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	//in < 0 mode, readdir returns all without an EOF error
	infos, err := f.Readdir(-1)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(infos) != 4 {
		t.Fatal("expected this many directory entries")
	}

	if infos[0].Name() != "a.txt" {
		t.Error("expected this file")
	}

	if infos[1].Name() != "b.txt" {
		t.Error("expected this file")
	}

	if infos[2].Name() != "bar" || infos[2].IsDir() != true {
		t.Error("expected this dir")
	}

	if infos[3].Name() != "bar\uFFFEc.txt" {
		t.Error("expected this file")
	}

}

func CaseFileReaddirLimitN(fs *FileSystem, t *testing.T) {
	testfiles(fs, t)

	f, err := fs.Open(Root)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	//in > 0 mode, readdir returns at most these number of fis
	infos, err := f.Readdir(2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(infos) != 2 {
		t.Error("expected this many directory entries")
	}
	//second call should also succeed, we have 4 entries
	infos2, err := f.Readdir(2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(infos2) != 2 {
		t.Error("expected this many directory entries")
	}

	//third call should fail with EOF
	infos3, err := f.Readdir(2)
	if err != io.EOF {
		t.Error("expected EOF for third readdir call")
	}

	if len(infos3) != 0 {
		t.Error("expected this many directory entries")
	}

	//new call should reset internal state
	infos4, err := f.Readdir(0)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(infos4) != 4 {
		t.Error("expected this many directory entries")
	}

	//newly reset internal state returns first 2 dirs again
	infos5, err := f.Readdir(2)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(infos5) != 2 {
		t.Error("expected this many directory entries")
	}
}

func CaseFileReaddirNamesAll(fs *FileSystem, t *testing.T) {
	testfiles(fs, t)

	f, err := fs.Open(Root)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	//in <= 0 mode, readdir returns all without an EOF error
	names, err := f.Readdirnames(0)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(names) != 4 {
		t.Fatal("expected this many directory names")
	}

	if names[0] != "a.txt" {
		t.Error("expected this name")
	}

	if names[1] != "b.txt" {
		t.Error("expected this name")
	}

	if names[2] != "bar" {
		t.Error("expected this name")
	}

	if names[3] != "bar\uFFFEc.txt" {
		t.Error("expected this name")
	}
}

func TestAtomicCases(t *testing.T) {

	cases := []struct {
		Name string
		Case func(fs *FileSystem, t *testing.T)
	}{
		{Name: "StatNonExisting", Case: CaseStatNonExisting},
		{Name: "StatNonExisting", Case: CaseStatExisting},
		{Name: "StatInvalidPath", Case: CaseStatInvalidPath},

		{Name: "OpenFileInvalidPath", Case: CaseOpenFileInvalidPath},
		{Name: "OpenFileCreateNonExisting", Case: CaseOpenFileCreateNonExisting},
		{Name: "OpenFileParentNotDirectory", Case: CaseOpenFileParentNotDirectory},
		{Name: "OpenFileParentNotExist", Case: CaseOpenFileParentNotExist},

		{Name: "OpenFileReadOnly", Case: CaseOpenFileReadOnly},
		{Name: "OpenFileExclusive", Case: CaseOpenFileExclusive},
		{Name: "OpenFileNonExisting", Case: CaseOpenFileNonExisting},

		{Name: "MkdirInvalidPath", Case: CaseMkdirInvalidPath},
		{Name: "MkdirNonExisting", Case: CaseMkdirNonExisting},
		{Name: "MkdirExistingFile", Case: CaseMkdirExistingFile},
		{Name: "MkdirParentNotDirectory", Case: CaseMkdirParentNotDirectory},
		{Name: "MkdirParentNotExist", Case: CaseMkdirParentNotExist},

		{Name: "FileReaddirAll", Case: CaseFileReaddirAll},
		{Name: "FileReaddirLimitN", Case: CaseFileReaddirLimitN},

		{Name: "FileReaddirNamesAll", Case: CaseFileReaddirNamesAll},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db, close := testdb(t)
			defer close()

			fs, err := NewFileSystem(t.Name(), db)
			if err != nil {
				t.Fatal(err)
			}

			c.Case(fs, t)
		})
	}
}

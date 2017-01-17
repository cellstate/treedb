package treedb

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

//Parent returns one level up from path components 'p' but if there are one or
//less components it will return the root
func Parent(p ...string) []string {
	if len(p) < 2 {
		return []string{}
	}

	return p[:len(p)-1]
}

//FileKey is used by the filesystem to transform path components into a datbase key
//if no path elements are provided it returns the root: just a single PathSeparator
func FileKey(p ...string) ([]byte, error) {
	n, err := FileName(p...)
	if err != nil {
		return nil, err
	}

	return []byte(n), nil
}

//FileName returns a full name (path) of the path components
func FileName(p ...string) (string, error) {
	if len(p) < 1 {
		return "", ErrInvalidPath
	}

	for _, c := range p {
		if strings.Contains(c, PathSeparator) {
			return "", ErrInvalidPath
		}
	}

	return PathSeparator + strings.Join(p, PathSeparator), nil
}

//FileInfo holds file metadata
type FileInfo struct {
	N string      // base name of the file
	S int64       // length in bytes for regular files; system-dependent for others
	M os.FileMode // file mode bits
	T time.Time   // modification time
	D bool        // abbreviation for Mode().IsDir()
}

//Name of the file
func (fi *FileInfo) Name() string { return fi.N }

//Size returns the number of bytes in a file
func (fi *FileInfo) Size() int64 { return fi.S }

//Mode returns a file's mode and permission bits. The bits have the
//same definition on all systems, so that information about files
//can be moved from one system to another portably. Not all bits apply to all
//systems. The only required bit is ModeDir for directories.
func (fi *FileInfo) Mode() os.FileMode { return fi.M }

//ModTime holds when the file was last modified
func (fi *FileInfo) ModTime() time.Time { return fi.T }

//IsDir reports whether m describes a directory. That is, it tests for the ModeDir bit being set in m.
func (fi *FileInfo) IsDir() bool { return fi.D }

//Sys returns underlying system values
func (fi *FileInfo) Sys() interface{} { return nil }

//FileSystem holds file information
type FileSystem struct {
	fbucket []byte //name of the files bucket

	db *bolt.DB
}

//File provides an handler for writing and reading
type File struct{}

//NewFileSystem sets up a new file system in a bolt database with
//an unique id that allows multiple filesystems per database
func NewFileSystem(id string, db *bolt.DB) (fs *FileSystem, err error) {
	fs = &FileSystem{
		fbucket: []byte("f_" + id),
		db:      db,
	}

	//create buckets
	if err = fs.db.Update(func(tx *bolt.Tx) (err error) {
		if _, err = tx.CreateBucketIfNotExists(fs.fbucket); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to prepare database: %v", err)
	}

	return fs, nil
}

func (fs *FileSystem) mightwrite(flag int) bool {
	//return whether the open() call might require a writeable transaction
	if flag&os.O_WRONLY != 0 || //might write file chunks
		flag&os.O_CREATE != 0 || //might create a file
		flag&os.O_RDWR != 0 { //might write file chunks
		return true
	}

	return false
}

func (fs *FileSystem) putfi(tx *bolt.Tx, k []byte, fi *FileInfo) (err error) {
	v, err := json.Marshal(fi)
	if err != nil {
		return fmt.Errorf("failed to serialize: %v", err)
	}

	return tx.Bucket(fs.fbucket).Put(k, v)
}

func (fs *FileSystem) getfi(tx *bolt.Tx, p P) (fi *FileInfo, err error) {
	v := tx.Bucket(fs.fbucket).Get(p.Key())
	if v == nil {
		return nil, os.ErrNotExist
	}

	fi = &FileInfo{}
	err = json.Unmarshal(v, fi)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize: %v", err)
	}

	return fi, nil
}

//Mkdir creates a new directory with the specified name and permission bits. If
//there is an error, it will be of type *PathError.
// func (fs *FileSystem) Mkdir(p ...string) (err error) {
// 	k, err := FileKey(p...)
// 	if err != nil {
// 		return err
// 	}
//
// 	_ = k
// 	return nil
// }

// Create creates the named file with mode 0666 (before umask), truncating
// it if it already exists. If successful, methods on the returned
// File can be used for I/O; the associated file descriptor has mode
// O_RDWR.
// If there is an error, it will be of type *PathError.
// func (fs *FileSystem) Create(p ...string) (*File, error) {
// 	return fs.OpenFile(os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666, p...)
// }

//OpenFile is the generalized open call. It opens the named file with specified
//flag (O_RDONLY etc.) and perm, (0666 etc.) if applicable. If successful,
//methods on the returned File can be used for I/O. If there is an error, it will
//be of type *PathError.
// O_RDONLY int = syscall.O_RDONLY // open the file read-only.
// O_WRONLY int = syscall.O_WRONLY // open the file write-only.
// O_RDWR   int = syscall.O_RDWR   // open the file read-write.
// O_APPEND int = syscall.O_APPEND // append data to the file when writing.
// O_SYNC   int = syscall.O_SYNC   // open for synchronous I/O.
// O_TRUNC  int = syscall.O_TRUNC  // if possible, truncate file when opened.
//
// O_CREATE int = syscall.O_CREATE  // create a new file if none exists.
// O_EXCL   int = syscall.O_EXCL   // used with O_CREATE, file must not exist
// func (fs *FileSystem) OpenFile(flag int, perm os.FileMode, p ...string) (*File, error) {
//
// 	//PART 1: Check if parent exists and is dir
// 	//	check: if parent exists OK, if not: "no such file or directory"
// 	//	check: if parent is not a directory: "not a directory"
//
// 	//Part 2: Check if file exists Create file file on the spot
// 	// if flag&O_CREATE != 0 { //create if not exists
// 	//    if flag&O_EXCL != 0 {
// 	//			//check: if exists: Err("already exists")
// 	//    	return nil, os.ErrExists
// 	// 		}
// 	//
// 	//    //fall through: it exists and we can open file
// 	// }
//
// 	//Part 3: Setup a file handle that can be returned for IO
//
// 	k, err := FileKey(p...)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	tx, err := fs.db.Begin(fs.mightwrite(flag))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to begin '%v' tx: %v", flag, err)
// 	}
//
// 	defer tx.Commit()
//
// 	//check if the parent directory exists
// 	parentk, err := FileKey(Parent(p...)...)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	parentfi, err := fs.getfi(tx, parentk)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			return nil, err
// 		}
//
// 		return nil, fmt.Errorf("failed to get parent fi: %v", err)
// 	}
//
// 	_ = parentfi
// 	//check if parent is a file
// 	// if !parentfi.IsDir() {
// 	// 	return nil, os.Err
// 	// }
//
// 	//@TODO check
//
// 	//@TODO check if directory exists
//
// 	//atomically check if the file already exists
// 	fi, err := fs.getfi(tx, k)
// 	if err == nil {
// 		//@TODO file already exists implementation
// 		return nil, fmt.Errorf("not implemented")
// 	} else if !os.IsNotExist(err) {
// 		return nil, fmt.Errorf("unexpected err during stat: %v", err)
// 	}
//
// 	//file doesnt exist, create the file if O_CREATE is set
// 	if flag&os.O_CREATE != 0 {
// 		fi = &FileInfo{
// 			N: p[len(p)-1], //basename
// 			M: perm,        //mode
// 			//@TODO fill in
// 		}
//
// 		err = fs.putfi(tx, k, fi)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to put file info for %s: %v", p, err)
// 		}
// 	} else {
// 		//@TODO implement no file creation
// 		return nil, fmt.Errorf("not implemented")
// 	}
//
// 	//if at this point we got a fi we we can setup file io
// 	if fi != nil {
// 		return &File{
// 		//@TODO populate file "handle"
// 		}, nil
// 	}
//
// 	return nil, fmt.Errorf("not implemented")
// }

//Stat returns a FileInfo describing the named file
func (fs *FileSystem) Stat(p P) (fi os.FileInfo, err error) {
	err = p.Validate()
	if err != nil {
		return nil, p.Err("validate", err)
	}

	if err = fs.db.View(func(tx *bolt.Tx) error {
		fi, err = fs.getfi(tx, p)
		if err != nil {
			return err
		}

		if fi == nil {
			return os.ErrNotExist
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return fi, nil
}

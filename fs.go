package treedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

var (
	//ErrNotDirectory is returned when a directory was expected
	ErrNotDirectory = errors.New("not a directory")
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

//fileInfo holds our specific file information
//and implements the os.FileInfo interface, the fields
//are public for easier JSON (un)marshalling
type fileInfo struct {
	N string      // base name of the file
	S int64       // length in bytes for regular files; system-dependent for others
	M os.FileMode // file mode bits
	T time.Time   // modification time
}

//Name of the file
func (fi *fileInfo) Name() string { return fi.N }

//Size returns the number of bytes in a file
func (fi *fileInfo) Size() int64 { return fi.S }

//Mode returns a file's mode and permission bits. The bits have the
//same definition on all systems, so that information about files
//can be moved from one system to another portably. Not all bits apply to all
//systems. The only required bit is ModeDir for directories.
func (fi *fileInfo) Mode() os.FileMode { return fi.M }

//ModTime holds when the file was last modified
func (fi *fileInfo) ModTime() time.Time { return fi.T }

//IsDir reports whether m describes a directory. That is, it tests for the ModeDir bit being set in m.
func (fi *fileInfo) IsDir() bool { return fi.Mode().IsDir() }

//Sys returns underlying system values
func (fi *fileInfo) Sys() interface{} { return nil }

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

	if err = fs.db.Update(func(tx *bolt.Tx) (err error) {
		if _, err = tx.CreateBucketIfNotExists(fs.fbucket); err != nil {
			return err
		}

		//create root (if its not yet created)
		_, err = fs.getfi(tx, Root)
		if err == os.ErrNotExist {
			if err = fs.putfi(tx, Root, &fileInfo{
				N: Root.Base(),
				M: os.ModeDir | 0777,
				//@TODO setup root file info completely
			}); err != nil {
				return err
			}
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

func (fs *FileSystem) putfi(tx *bolt.Tx, p P, fi *fileInfo) (err error) {
	v, err := json.Marshal(fi)
	if err != nil {
		return fmt.Errorf("failed to serialize: %v", err)
	}

	return tx.Bucket(fs.fbucket).Put(p.Key(), v)
}

func (fs *FileSystem) getfi(tx *bolt.Tx, p P) (fi *fileInfo, err error) {
	v := tx.Bucket(fs.fbucket).Get(p.Key())
	if v == nil {
		return nil, os.ErrNotExist
	}

	fi = &fileInfo{}
	err = json.Unmarshal(v, fi)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize: %v", err)
	}

	return fi, nil
}

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
func (fs *FileSystem) Mkdir(p P) (err error) {
	k, err := FileKey(p...)
	if err != nil {
		return p.Err("mkdir", err)
	}

	_ = k
	return nil
}

// Create creates the named file with mode 0666 (before umask), truncating
// it if it already exists. If successful, methods on the returned
// File can be used for I/O; the associated file descriptor has mode
// O_RDWR. If there is an error, it will be of type *PathError.
func (fs *FileSystem) Create(p P) (*File, error) {
	return fs.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// OpenFile is the generalized open call. It opens the named file with specified
// flag (O_RDONLY etc.) and perm, (0666 etc.) if applicable. If successful,
// methods on the returned File can be used for I/O. If there is an error, it will
// be of type *PathError. Behaviour can be customized with the following flags:
//   O_RDONLY int = syscall.O_RDONLY // open the file read-only.
//   O_WRONLY int = syscall.O_WRONLY // open the file write-only.
//   O_RDWR   int = syscall.O_RDWR   // open the file read-write.
//   O_APPEND int = syscall.O_APPEND // append data to the file when writing.
//   O_SYNC   int = syscall.O_SYNC   // open for synchronous I/O.
//   O_TRUNC  int = syscall.O_TRUNC  // if possible, truncate file when opened.
//   O_CREATE int = syscall.O_CREATE  // create a new file if none exists.
//   O_EXCL   int = syscall.O_EXCL   // used with O_CREATE, file must not exist
func (fs *FileSystem) OpenFile(p P, flag int, perm os.FileMode) (f *File, err error) {
	err = p.Validate()
	if err != nil {
		return nil, p.Err("open", err)
	}

	//begin the transaction
	tx, err := fs.db.Begin(fs.mightwrite(flag))
	if err != nil {
		return nil, err
	}

	//always end the transaction
	defer func() {
		if cerr := tx.Commit(); cerr != nil {
			err = cerr //commit errors will take precedence
		}
	}()

	//attempt to get existing file
	fi, err := fs.getfi(tx, p)
	if err != nil {
		if err != os.ErrNotExist {
			return nil, p.Err("open", err) //something unexpected went wrong
		}
	}

	//do we want to create (if it doesnt exist)
	if flag&os.O_CREATE != 0 {
		if fi == nil {

			//make sure parent exists
			pp := p.Parent()
			pfi, err := fs.getfi(tx, pp)
			if err != nil {
				return nil, pp.Err("open", err) //report both ErrNotExist the same here
			}

			//make sure it is a directory
			if !pfi.IsDir() {
				return nil, pp.Err("open", ErrNotDirectory)
			}

			//setup new file
			fi = &fileInfo{
				N: p.Base(),
				M: perm,
				//@TODO create valid file info
			}

			//insert it
			if err = fs.putfi(tx, p, fi); err != nil {
				return nil, p.Err("open", err)
			}

		} else if flag&os.O_EXCL != 0 {
			return nil, p.Err("open", os.ErrExist) //it existed, but user wants exclusive access
		}
	}

	//at this point we expect a file to exist
	if fi == nil {
		return nil, p.Err("open", os.ErrNotExist)
	}

	//Setup IO to the actual file
	//@TODO How do we represent a file handle in our system?
	//transaction for each block? custom locking flag?
	f = &File{
	//@TODO create io ready file
	}

	return f, nil
}

//Stat returns a FileInfo describing the named file
func (fs *FileSystem) Stat(p P) (fi os.FileInfo, err error) {
	err = p.Validate()
	if err != nil {
		return nil, p.Err("stat", err)
	}

	if err = fs.db.View(func(tx *bolt.Tx) error {
		fi, err = fs.getfi(tx, p)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, p.Err("stat", err)
	}

	return fi, nil
}

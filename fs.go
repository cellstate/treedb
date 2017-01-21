package treedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

var (
	//ErrNotDirectory is returned when a directory was expected
	ErrNotDirectory = errors.New("not a directory")
	//ErrNotEmptyDirectory tells us the directory was not empty
	ErrNotEmptyDirectory = errors.New("directory is not empty")
)

//fileInfo holds our specific file information
//and implements the os.FileInfo interface, the fields
//are public for easier JSON (un)marshalling
type fileInfo struct {
	N string      // base name of the file
	M os.FileMode // file mode bits
	T time.Time   // modification time
	S int64       // length in bytes for regular files; system-dependent for others
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

//walkFn can be provided t
type walkFn func(p P, fi *fileInfo) (err error)

//errStopWalk can be returned  by the walkFn to stop iterating a directory
var errStopWalk = errors.New("stop walk")

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
				T: time.Now(),
				//@TODO setup size
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
	//@TODO figure out if file writes still cause the transaction to be writeable
	if flag&os.O_WRONLY != 0 || //might write file chunks
		flag&os.O_CREATE != 0 || //might create a file
		flag&os.O_RDWR != 0 { //might write file chunks
		return true
	}

	return false
}

func (fs *FileSystem) walkdir(tx *bolt.Tx, p P, startp P, fn walkFn) (err error) {
	c := tx.Bucket(fs.fbucket).Cursor()
	prefix := p.Key()

	//we can start walking from a different item if startp is not nitl, this
	//is used by readdir to continue from a path it left off
	start := prefix
	if startp != nil {
		start = startp.Key()
	}

	for k, v := c.Seek(start); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if bytes.Equal(start, k) {
			continue
		}

		parts := bytes.SplitN(bytes.TrimPrefix(k, prefix), []byte(PathSeparator), 2)
		if len(parts) > 1 {
			break //end of the directory
		}

		fi := &fileInfo{}
		err = json.Unmarshal(v, fi)
		if err != nil {
			return fmt.Errorf("failed to deserialize: %v", err)
		}

		childp := PathFromKey(k)
		err = fn(childp, fi)
		if err != nil {
			if err == errStopWalk {
				return nil
			}

			return err
		}
	}

	return nil
}

func (fs *FileSystem) delfi(tx *bolt.Tx, p P) (err error) {
	return tx.Bucket(fs.fbucket).Delete(p.Key())
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

// Remove removes the named file or directory.
// If there is an error, it will be of type *PathError.
func (fs *FileSystem) Remove(p P) (err error) {
	err = p.Validate()
	if err != nil {
		return p.Err("remove", err)
	}

	if err = fs.db.Update(func(tx *bolt.Tx) error {

		//must exist for remove to succeed
		fi, err := fs.getfi(tx, p)
		if err != nil {
			return err
		}

		//if its a directory, its must be empty
		if fi.IsDir() {
			empty := true
			if err = fs.walkdir(tx, p, nil, func(pp P, childfi *fileInfo) error {
				//if this is called at least one time, the dir is not empty, we  dont need to know more
				empty = false
				return errStopWalk
			}); err != nil {
				return err //error while walking
			}

			if !empty {
				return ErrNotEmptyDirectory
			}
		}

		//actually remove the item, open file handles might still perform io
		return fs.delfi(tx, p)
	}); err != nil {
		return p.Err("remove", err)
	}

	return nil
}

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
func (fs *FileSystem) Mkdir(p P, perm os.FileMode) (err error) {
	err = p.Validate()
	if err != nil {
		return p.Err("mkdir", err)
	}

	//begin the transaction
	tx, err := fs.db.Begin(true)
	if err != nil {
		return err
	}

	//always end the transaction
	defer func() {
		if cerr := tx.Commit(); cerr != nil {
			err = cerr //commit errors will take precedence
		}
	}()

	//check if parent exists
	pp := p.Parent()
	pfi, err := fs.getfi(tx, pp)
	if err != nil {
		return pp.Err("mkdir", err) //no parent or some other problem with its path
	}

	//check if its a directory
	if !pfi.IsDir() {
		return pp.Err("mkdir", ErrNotDirectory)
	}

	//check if the directory already exists
	fi, err := fs.getfi(tx, p)
	if err != nil {
		if err != os.ErrNotExist {
			return p.Err("mkdir", err)
		}

		//dir doesnt exist; create it
		fi = &fileInfo{
			N: p.Base(),
			M: os.ModeDir | perm,
			T: time.Now(),
			//@TODO complete information
		}

		//and insert the record
		if err = fs.putfi(tx, p, fi); err != nil {
			return p.Err("mkdir", err)
		}

	} else {
		if !fi.IsDir() {
			//dir exists but is not a directory
			return p.Err("mkdir", os.ErrExist)
		}
	}

	return nil
}

// Open opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func (fs *FileSystem) Open(p P) (*File, error) {
	return fs.OpenFile(p, os.O_RDONLY, 0)
}

// OpenFile is the generalized open call. It opens the named file with specified
// flag (O_RDONLY etc.) and perm, (0666 etc.) if applicable. If successful,
// methods on the returned File can be used for I/O. If there is an error, it will
// be of type *PathError. Behaviour can be customized with the following flags:
//
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
		if !tx.Writable() {
			return
		}

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
				return nil, pp.Err("open", err) //report both ErrNotExist and other errors the same
			}

			//make sure it is a directory
			if !pfi.IsDir() {
				return nil, pp.Err("open", ErrNotDirectory)
			}

			//setup new file
			fi = &fileInfo{
				N: p.Base(),
				M: perm,
				T: time.Now(),
				//@TODO setup determine size
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

	//finally set up the file (handle) with available info
	return NewFile(fs, p), nil
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

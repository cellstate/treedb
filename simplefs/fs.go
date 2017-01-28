package simplefs

import (
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

//K is a content-based key
type K [sha256.Size]byte

var (
	//ZeroKey is an content key with only 0x00 bytes
	ZeroKey = K{}
)

//FileSystem provides a filesystem abstraction on top of Bolt db
type FileSystem struct {
	db   *bolt.DB
	root uint64
}

//New creates a simple filesystem on the provided database
func New(db *bolt.DB) (fs *FileSystem, err error) {
	fs = &FileSystem{
		db:   db,
		root: 1, //@TODO make this more flexible
	}

	if err = fs.db.Update(func(tx *bolt.Tx) (err error) {
		var b *bolt.Bucket
		if b, err = tx.CreateBucketIfNotExists(NodeBucketName); err != nil {
			return err
		}

		//create root node if it doesnt exist
		v := b.Get(u64tob(fs.root))
		if v == nil {
			ntx, err := newNodeTx(tx, 0)
			if err != nil {
				return err
			}

			fs.root, _, err = ntx.putNode(os.ModeDir | 0777)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to prepare database: %v", err)
	}

	return fs, nil
}

func (fs *FileSystem) stat(tx *bolt.Tx, p P) (fi *fileInfo, err error) {
	ntx, err := newNodeTx(tx, fs.root)
	if err != nil {
		return nil, fmt.Errorf("failed to create node tx for '%v': %v", fs.root, err)
	}

	nid := ntx.getDescendantID(p)
	if nid == 0 {
		return nil, os.ErrNotExist
	}

	ntx, err = newNodeTx(tx, nid)
	if err != nil {
		return nil, fmt.Errorf("failed to create node tx for '%v': %v", nid, err)
	}

	n, err := ntx.getNode()
	if err != nil {
		return nil, err
	}

	if n == nil {
		return nil, os.ErrNotExist
	}

	return newFileInfo(p.Base(), n, nid), nil
}

//Stat returns a FileInfo describing the named file. If there is an error, it will be of type *PathError.
func (fs *FileSystem) Stat(p P) (fi os.FileInfo, err error) {
	err = p.Validate()
	if err != nil {
		return nil, p.Err("stat", err)
	}

	if err = fs.db.View(func(tx *bolt.Tx) error {
		fi, err = fs.stat(tx, p)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, p.Err("stat", err)
	}

	return fi, nil
}

func (fs *FileSystem) mkdir(tx *bolt.Tx, p P, perm os.FileMode) (err error) {
	pp := p.Parent()

	//check if parent exists
	pfi, err := fs.stat(tx, pp)
	if err != nil {
		return err
	}

	//check if its a directory
	if !pfi.IsDir() {
		return ErrNotDirectory
	}

	//check if the directory itself already exists
	fi, err := fs.stat(tx, p)
	if err != nil {
		if err != os.ErrNotExist {
			return err
		}

		//@TODO find out if parent cascading below can be generalized
		ntx, err := newNodeTx(tx, 0)
		if err != nil {
			return fmt.Errorf("failed to start new node tx: %v", err)
		}

		nodeID, _, err := ntx.putNode(os.ModeDir | perm)
		if err != nil {
			return fmt.Errorf("failed to put new node: %v", err)
		}

		pntx, err := newNodeTx(tx, pfi.nodeID)
		if err != nil {
			return fmt.Errorf("failed to start parent node tx: %v", err)
		}

		err = pntx.putChildPtr(p.Base(), nodeID)
		if err != nil {
			return fmt.Errorf("failed to put child ptr: %v", err)
		}

		_, _, err = pntx.putNode(pfi.Mode())
		if err != nil {
			return fmt.Errorf("failed to update parent node: %v", err)
		}

	} else {
		if !fi.IsDir() {
			//node at path exists but is not a directory
			return os.ErrExist
		}
	}

	return nil
}

// Mkdir creates a new directory with the specified name and permission bits. If there is an error, it will be of type *PathError.
func (fs *FileSystem) Mkdir(p P, perm os.FileMode) (err error) {
	err = p.Validate()
	if err != nil {
		return p.Err("mkdir", err)
	}

	if err = fs.db.Update(func(tx *bolt.Tx) error {
		err = fs.mkdir(tx, p, perm)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return p.Err("mkdir", err)
	}

	return nil
}

func (fs *FileSystem) openFile(tx *bolt.Tx, p P, flag int, perm os.FileMode) (f *File, err error) {

	fi, err := fs.stat(tx, p)
	if err != nil {
		if err != os.ErrNotExist {
			return nil, err //something unexpected went wrong
		}
	}

	//do we want to create (if it doesnt exist)
	if flag&os.O_CREATE != 0 {
		if fi == nil {

			//@TODO generalize "addNode" logic below(?):
			pp := p.Parent()

			//check if parent exists
			pfi, err := fs.stat(tx, pp)
			if err != nil {
				return nil, err
			}

			//check if its a directory
			if !pfi.IsDir() {
				return nil, ErrNotDirectory
			}

			ntx, err := newNodeTx(tx, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to start new node tx: %v", err)
			}

			nodeID, n, err := ntx.putNode(perm)
			if err != nil {
				return nil, fmt.Errorf("failed to put new node: %v", err)
			}

			pntx, err := newNodeTx(tx, pfi.nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to start parent node tx: %v", err)
			}

			err = pntx.putChildPtr(p.Base(), nodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to put child ptr: %v", err)
			}

			_, _, err = pntx.putNode(pfi.Mode())
			if err != nil {
				return nil, fmt.Errorf("failed to update parent node: %v", err)
			}

			fi = newFileInfo(p.Base(), n, nodeID)
		} else if flag&os.O_EXCL != 0 {
			return nil, os.ErrExist //it existed, but user wants exclusive access
		}
	}

	//at this point we expect some file information
	if fi == nil {
		return nil, os.ErrNotExist
	}

	return f, nil
}

// OpenFile is the generalized open call. It opens the named file with specified flag (O_RDONLY etc.) and perm, (0666 etc.) if applicable. If successful, methods on the returned File can be used for I/O. If there is an error, it will be of type *PathError. Behaviour can be customized with the following flags:
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
	//@TODO dont use a writeable transaction if flags indicate read only
	tx, err := fs.db.Begin(true)
	if err != nil {
		return nil, err
	}

	f, err = fs.openFile(tx, p, flag, perm)
	if err != nil {
		return nil, p.Err("open", err)
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

	return f, nil
}

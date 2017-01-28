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

	return &fileInfo{name: p.Base(), node: n}, nil
}

//Stat returns a FileInfo describing the named file
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

// Mkdir creates a new directory with the specified name and permission bits. If
// there is an error, it will be of type *PathError.
// func (fs *FileSystem) Mkdir(p P, perm os.FileMode) (err error) {
// 	err = p.Validate()
// 	if err != nil {
// 		return p.Err("mkdir", err)
// 	}
//
// 	//begin the transaction
// 	tx, err := fs.db.Begin(true)
// 	if err != nil {
// 		return err
// 	}
//
// 	//always end the transaction
// 	defer func() {
// 		if cerr := tx.Commit(); cerr != nil {
// 			err = cerr //commit errors will take precedence
// 		}
// 	}()
//
// 	//check if parent exists
// 	// pp := p.Parent()
//
// 	// pntx, err := newNodeTx(tx, id)
//
// 	// pfi, err := fs.getfi(tx, pp)
// 	// if err != nil {
// 	// 	return pp.Err("mkdir", err) //no parent or some other problem with its path
// 	// }
//
// 	//check if its a directory
// 	// if !pfi.IsDir() {
// 	// 	return pp.Err("mkdir", ErrNotDirectory)
// 	// }
//
// 	//check if the directory already exists
// 	// fi, err := fs.getfi(tx, p)
// 	// if err != nil {
// 	// 	if err != os.ErrNotExist {
// 	// 		return p.Err("mkdir", err)
// 	// 	}
// 	//
// 	// 	// //dir doesnt exist; create it
// 	// 	// fi = &fileInfo{
// 	// 	// 	N: p.Base(),
// 	// 	// 	M: os.ModeDir | perm,
// 	// 	// 	T: time.Now(),
// 	// 	// 	//@TODO complete information
// 	// 	// }
// 	//
// 	// 	//and insert the record
// 	// 	// if err = fs.putfi(tx, p, fi); err != nil {
// 	// 	// 	return p.Err("mkdir", err)
// 	// 	// }
// 	//
// 	// } else {
// 	// 	if !fi.IsDir() {
// 	// 		//dir exists but is not a directory
// 	// 		return p.Err("mkdir", os.ErrExist)
// 	// 	}
// 	// }
//
// 	return nil
// }

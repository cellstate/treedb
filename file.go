package treedb

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

//K is the content hash of a file chunk
type K [sha256.Size]byte

//File provides an handler for IO, it is not safe
//for concurrent writing. It works with an internal
//cursor that can be written to and read from
//Dakony:
// - ReadFile(ctx context.Context, fi *FileInfo, bs []byte, offset int64) (int, error)
// - WriteFile(ctx context.Context, fi *FileInfo, bs []byte, offset int64) (int, error)
// - FlushFileBuffers(ctx context.Context, fi *FileInfo) error
//FUSE:
// - fs.HandleReader:
// type ReadRequest struct {
//     Header    `json:"-"`
//     Dir       bool // is this Readdir?
//     Handle    HandleID
//     Offset    int64
//     Size      int
//     Flags     ReadFlags
//     LockOwner uint64
//     FileFlags OpenFlags
// }
// - fs.HandleWriter
// - fs.HandleFlusher
type File struct {
	p      P           //path as passed to open
	fs     *FileSystem //file system this file is part of
	chunks map[int64]K //maps chunk file position (bytes) to chunk k

	//TODO rq: how do we handle db transactions for long reads (cant block the whole db)
	//TODO rq: how do we update modtimes
	//TODO rq: how to do appending
	//TODO implement: read dir
	//TODO what to do if two threads opens same file?
}

//NewFile sets up a file on filesystem 'fs' at path 'p'
func NewFile(fs *FileSystem, p P) *File {
	return &File{
		fs: fs,
		p:  p,
	}
}

// Readdir reads the contents of the directory associated with file and returns a slice of up to n FileInfo values, as would be returned by Lstat, in directory order. Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if Readdir returns an empty slice, it will return a non-nil error explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in a single slice. In this case, if Readdir succeeds (reads all the way to the end of the directory), it returns the slice and a nil error. If it encounters an error before the end of the directory, Readdir returns the FileInfo read until that point and a non-nil error.
func (f *File) Readdir(n int) (fi []os.FileInfo, err error) {

	if err = f.fs.db.View(func(tx *bolt.Tx) error {

		c := tx.Bucket(f.fs.fbucket).Cursor()
		prefix := f.p.Key()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	//@TODO implement
	return fi, nil
}

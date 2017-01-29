package simplefs

import (
	"io"

	"github.com/restic/chunker"
)

const kiB = 1024
const miB = kiB * 1024

//@TODO what about concurrent file writing/reading?
//@TODO can we do better then linux: http://0pointer.de/blog/projects/locking.html

//File represents a handle for writing and reading
type File struct {
	buf    []byte
	chkr   *chunker.Chunker
	pol    chunker.Pol
	Pw     io.WriteCloser
	chunks map[uint][]byte

	fs  *FileSystem //filesystem this file is on
	nid uint64      //id of the node this handle is responsible for
}

//NewFile creates an interface for writing and reading byte chunks through a traditional file interface
func NewFile(fs *FileSystem, nodeID uint64) *File {
	f := &File{
		fs:     fs,
		nid:    nodeID,
		pol:    chunker.Pol(0x3DA3358B4DC173),
		chunks: map[uint][]byte{},
	}

	var pr io.Reader
	pr, f.Pw = io.Pipe()

	f.chkr = chunker.NewWithBoundaries(pr, f.pol, (256 * kiB), (1 * miB))
	f.buf = make([]byte, f.chkr.MaxSize)

	go func() {
		for {
			chunk, err := f.chkr.Next(f.buf)
			if err == io.EOF {
				break
			}

			f.chunks[chunk.Start] = make([]byte, chunk.Length)
			copy(f.chunks[chunk.Start], chunk.Data)
		}
	}()

	return f
}

// Write writes len(b) bytes to the File. It returns the number of bytes written and an error, if any. Write returns a non-nil error when n != len(b).
func (f *File) Write(b []byte) (n int, err error) {
	n, err = f.Pw.Write(b)
	if err != nil {
		return n, err
	}

	return n, err
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes read and an error, if any. EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(b []byte) (n int, err error) {
	return 0, ErrNotImplemented
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted according to whence: 0 means relative to the origin of the file, 1 means relative to the current offset, and 2 means relative to the end. It returns the new offset and an error, if any. The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (ret int64, err error) {

	//seek forces us to close the chunk writer to flush stored bytes of the chunker to our map.

	//the chunker should then be reset and start overwriting

	return 0, ErrNotImplemented
}

//Sync will commit in-memory chunks to the database, from there its up to the OS and disk hardware to make sure it arrives on the actual medium
func (f *File) Sync() error {
	return ErrNotImplemented
}

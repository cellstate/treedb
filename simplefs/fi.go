package simplefs

import (
	"os"
	"time"
)

//fileInfo holds our specific file information
//and implements the os.FileInfo interface, the fields
//are public for easier JSON (un)marshalling
type fileInfo struct {
	name string // base name of the file, inferred from path
	node *node  //underlying persistent node
}

//Name of the file
func (fi *fileInfo) Name() string { return fi.name }

//Size returns the number of bytes in a file
func (fi *fileInfo) Size() int64 { return fi.node.Size }

//Mode returns a file's mode and permission bits. The bits have the
//same definition on all systems, so that information about files
//can be moved from one system to another portably. Not all bits apply to all
//systems. The only required bit is ModeDir for directories.
func (fi *fileInfo) Mode() os.FileMode { return fi.node.Mode }

//ModTime holds when the file was last modified
func (fi *fileInfo) ModTime() time.Time { return fi.node.ModTime }

//IsDir reports whether m describes a directory. That is, it tests for the ModeDir bit being set in m.
func (fi *fileInfo) IsDir() bool { return fi.Mode().IsDir() }

//Sys returns underlying system values
func (fi *fileInfo) Sys() interface{} { return nil }

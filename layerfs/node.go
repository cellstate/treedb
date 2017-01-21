package layerfs

import (
	"os"
	"time"
)

//A Node can either contain links to other nodes (a directory) or to opaque content chunks when representing a file. Node implements os.FileInfo.
type Node struct {
	N string      //base name
	T time.Time   //mod time
	S int64       //size
	M os.FileMode //portable mode bits

	Children []K         //ordered list of sub nodes @TODO do we want to load this all into memory or abstract some buffer?
	Chunks   map[int64]K //maps a chunks file offset to the chunk's key @TODO do we want to load this all into memory or abstract some buffer?
}

//Name of the file
func (n *Node) Name() string { return n.N }

//Size returns the number of bytes in a file
func (n *Node) Size() int64 { return n.S }

//Mode returns a file's mode and permission bits. The bits have the
//same definition on all systems, so that information about files
//can be moved from one system to another portably. Not all bits apply to all
//systems. The only required bit is ModeDir for directories.
func (n *Node) Mode() os.FileMode { return n.M }

//ModTime holds when the file was last modified
func (n *Node) ModTime() time.Time { return n.T }

//IsDir reports whether m describes a directory. That is, it tests for the ModeDir bit being set in m.
func (n *Node) IsDir() bool { return n.Mode().IsDir() }

//Sys returns underlying system values
func (n *Node) Sys() interface{} { return nil }

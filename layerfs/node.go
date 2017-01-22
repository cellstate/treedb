package layerfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

//BranchWriter acts as a handle for modifying a branch Node in our hierarchy. Upon initiating, the key of the node will be determined although an actual value for this key will only be written upon committing. Operations on the node can span different database transactions.
type BranchWriter struct {
	k         []byte
	mChildren map[string][]byte
}

//NewBranchWriter allow writing a (new) branch node while merging children 'mChildren' and chunks 'mChunks' with the existing node at key 'nodeK'.
//
//When the node's key 'k' is nil a new node is instead created with just the provided children 'mChildren' and chunks 'mChunks'
//
//When the key of a merged child is a ZeroKey the child acts as a tombstone and is removed from the new node instead.
//
//When the first new chunk is mapped to a lower file offset then existing chunks only chunks positioned lower than the new offset are copied over. If the first merged chunk has an offset of zero we are rewriting the node completely, and if then the first chunk has a zerokey we are truncating the node's content.
func NewBranchWriter(k []byte, tx *bolt.Tx, mChildren map[string][]byte) (*BranchWriter, error) {

	if k == nil {
		b := tx.Bucket(NodeBucketName)
		nexti, err := b.NextSequence()
		if err != nil {
			return nil, err
		}

		k = u64tob(nexti)
	}

	return &BranchWriter{
		k:         k,
		mChildren: mChildren,
	}, nil
}

//WriteChild will write a reference to child node at 'k' in the branch node
func (nw *BranchWriter) WriteChild(tx *bolt.Tx, name string, k []byte) error {
	return tx.
		Bucket(NodeBucketName).
		Put(bytes.Join([][]byte{nw.k, []byte(name)}, []byte(PathSeparator)), k)
}

//Commit the branch node with its, merged children while serialize file information and calculate the final checksum, the size field 'S' and modTime filed 'T' will be set by the commit.
func (nw *BranchWriter) Commit(tx *bolt.Tx, n *Node) (err error) {
	b := tx.Bucket(NodeBucketName)

	//start writing child keys, prefixed with this new keys such that seeks can easily traverse down the tree.
	for name, childk := range nw.mChildren {
		if err = nw.WriteChild(tx, name, childk); err != nil {
			return err
		}
	}

	//@TODO write chunks, make sure ordering based on offset is correct
	//@TODO support truncation, appending and partial differences
	// for offset, chunkk := range mChunks {}
	//@TODO copy over old children, unless tombstones

	//we now read back everything we wrote (all stuff prefixed with key 'k') to compute the node's checksum, boltdb makes sure everything is ordered consistently
	c := b.Cursor()
	h := sha256.New()
	for kk, v := c.Seek(nw.k); kk != nil && bytes.HasPrefix(kk, nw.k); kk, v = c.Next() {

		nwritten, err := h.Write(v)
		if err != nil || nwritten != len(v) {
			return fmt.Errorf("failed to hash new node's content: %v", err)
		}

		//a branch's size is sum of all keys
		n.S = n.S + int64(nwritten)

		fmt.Println(kk, v)
	}

	//serialize the node with the latest modification time
	n.T = time.Now()
	data, err := json.Marshal(n)
	if err != nil {
		return ErrSerialize
	}

	//write checksum and data to a buffer
	buf := bytes.NewBuffer(h.Sum(data))
	nwritten, err := buf.Write(data)
	if err != nil || nwritten != len(data) {
		return fmt.Errorf("failed to write serialized to buf: %v", err)
	}

	//finally write the checksummed node
	return b.Put(nw.k, buf.Bytes())
}

//A Node can either contain links to other nodes (a directory) or to opaque content chunks when representing a file. Node implements os.FileInfo. It is stores as a header under the node's key (hash), directly followed by a list of its nested values (other nodes for branches, chunk offsets for a leaf):
// |           Key      |       Data             |       Comment			 				 |
// 00000001						  : <CHECKSUM>{ ... }      #file info, checksum as prefix
// 00000001/a.txt			  : 00000002    					 #to another node
// 00000001/b.txt			  : 00000003    					 #to another node
// 00000001:0						: 2511E0F94...979AF0F    #chunk at file offset 0
// 00000001:332111			: 2511E0F94...979AF0F  	 #chunk at offset 332111
type Node struct {
	N string      //base name
	T time.Time   //mod time
	S int64       //size
	M os.FileMode //portable mode bits
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

package simplefs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

var (
	//ChunkPtrSeparator separates a node key from a chunk offset
	ChunkPtrSeparator = []byte(":")

	//ChildPtrSeparator separates a node key from a child name
	ChildPtrSeparator = []byte("/")
)

// u64tob converts a uint64 into an 8-capacity byte slice. From the author of bolt on sequential writes: https://github.com/boltdb/bolt/issues/338
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func childPtrKey(id uint64, name string) (k []byte) {
	k = u64tob(id)
	k = append(k, ChildPtrSeparator...)
	if name != "" {
		k = append(k, []byte(name)...)
	}

	return k
}

//low level node information, similar to a linux inode. Stored as
//
// |           Key      |       Data             |       Comment			 				 |
// 00000001						  : { ... }                #node info (a directory)
// 00000001/a.txt			  : 00000002    					 #to another node
// 00000001/b.txt			  : 00000003    					 #to another node
// 00000002						  : { ... }                #node info (a file)
// 00000002:0						: 2511E0F94...979AF0F    #chunk at file offset 0
// 00000003						  : { ... }                #node info (a file)
// 00000003:0						: 2511E0F94...979AF0F    #chunk at file offset 0 (dedup)
type node struct {
	Size    int64       `json:"s"` // node size in bytes
	Mode    os.FileMode `json:"m"` // file mode bits
	ModTime time.Time   `json:"t"` // modification time
}

//used for reading and writing low-level nodes
type nodeTx struct {
	id uint64
	tx *bolt.Tx
}

//start a new node interaction. If id == 0, a new node id is generated. This effectively creates a new node.
func newNodeTx(tx *bolt.Tx, id uint64) (ntx *nodeTx, err error) {
	if id == 0 {
		id, err = tx.Bucket(FileBucketName).NextSequence()
		if err != nil {
			return nil, err
		}
	}

	return &nodeTx{id: id, tx: tx}, nil
}

//getDecendantID will descend into subnodes following path 'p'
func (ntx *nodeTx) getDescendantID(p P) (id uint64) {
	id = ntx.id
	for _, comp := range p {
		k := childPtrKey(id, comp)
		v := ntx.tx.Bucket(FileBucketName).Get(k)
		if v == nil {
			return 0
		}

		id = btou64(v)
	}

	return id
}

//putChunkPtr writes a prefixed key that points to a content-based chunk key
func (ntx *nodeTx) putChunkPtr(offset int64, k [sha256.Size]byte) (err error) {
	//1. create key using ntx.id and offset
	//2. write content-based chunk key 'k' as value under db key
	return ErrNotImplemented
}

//getChildPtrs will scan the children of node (if any) and call 'fn' for each
func (ntx *nodeTx) getChildPtrs(fn func(name string, id uint64) error) (err error) {
	c := ntx.tx.Bucket(FileBucketName).Cursor()
	prefix := childPtrKey(ntx.id, "")
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		name := bytes.TrimPrefix(k, prefix)
		err = fn(string(name), btou64(v))
		if err != nil {
			return err
		}
	}

	return nil
}

//putChildPtr writes a prefixed key that points to another node
func (ntx *nodeTx) putChildPtr(name string, id uint64) (err error) {
	err = ntx.tx.Bucket(FileBucketName).Put(childPtrKey(ntx.id, name), u64tob(id))
	if err != nil {
		return fmt.Errorf("failed to put child ptr in %v: %v", ntx.id, err)
	}

	return nil
}

//putInfo completes, serializes and (over)writes the actual node key in the db
func (ntx *nodeTx) putNode(mode os.FileMode) (id uint64, n *node, err error) {
	n = &node{
		Size:    0,          //@TODO cursor over node ptrs to refresh this
		Mode:    mode,       //@TODO infer from presensense of childPtr/chunkPtr?
		ModTime: time.Now(), //@TODO only update if things changed (add checksum)?
	}

	d, err := json.Marshal(n)
	if err != nil {
		return 0, nil, ErrSerialize
	}

	err = ntx.tx.Bucket(FileBucketName).Put(u64tob(ntx.id), d)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to put node %v: %v", ntx.id, err)
	}

	return ntx.id, n, nil
}

//getNode deserializes the node information and returns it
func (ntx *nodeTx) getNode() (n *node, err error) {
	v := ntx.tx.Bucket(FileBucketName).Get(u64tob(ntx.id))
	if v == nil {
		return nil, nil
	}

	n = &node{}
	err = json.Unmarshal(v, n)
	if err != nil {
		return nil, ErrDeserialize
	}

	return n, nil
}

package simplefs

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

var (
	//NodeBucketName is the name of the bucket that will hold all nodes
	NodeBucketName = []byte("nodes")
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

//format a database key for a node's child ptr
func childPtrKey(id uint64, name string) (k []byte) {
	k = u64tob(id)
	k = append(k, ChildPtrSeparator...)
	if name != "" {
		k = append(k, []byte(name)...)
	}

	return k
}

//format a database key for a node's chunk ptr
func chunkPtrKey(id uint64, offset int64) (k []byte) {
	k = append(u64tob(id), ChunkPtrSeparator...)
	if offset < 0 {
		return k
	}

	buf := make([]byte, 8)
	binary.PutVarint(buf, offset)
	return append(k, buf...)
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
		id, err = tx.Bucket(NodeBucketName).NextSequence()
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
		v := ntx.tx.Bucket(NodeBucketName).Get(k)
		if v == nil {
			return 0
		}

		id = btou64(v)
	}

	return id
}

//getChunkPtrs will scan the children of node (if any) and call 'fn' for each
func (ntx *nodeTx) getChunkPtrs(fn func(offset int64, k K) error) (err error) {
	c := ntx.tx.Bucket(NodeBucketName).Cursor()
	prefix := chunkPtrKey(ntx.id, -1)
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		offsetb := bytes.TrimPrefix(k, prefix)
		offset, _ := binary.Varint(offsetb)
		ptrk := K{}
		copy(ptrk[:], v)

		err = fn(offset, ptrk)
		if err != nil {
			return err
		}
	}

	return nil
}

//putChunkPtr writes a prefixed key that points to a content-based chunk key
func (ntx *nodeTx) putChunkPtr(offset int64, k K) (err error) {
	err = ntx.tx.Bucket(NodeBucketName).Put(chunkPtrKey(ntx.id, offset), k[:])
	if err != nil {
		return fmt.Errorf("failed to put chunk ptr in %v: %v", ntx.id, err)
	}

	return nil
}

//getChildPtrs will scan the children of node (if any) and call 'fn' for each
func (ntx *nodeTx) getChildPtrs(fn func(name string, id uint64) error) (err error) {
	c := ntx.tx.Bucket(NodeBucketName).Cursor()
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
	err = ntx.tx.Bucket(NodeBucketName).Put(childPtrKey(ntx.id, name), u64tob(id))
	if err != nil {
		return fmt.Errorf("failed to put child ptr in %v: %v", ntx.id, err)
	}

	return nil
}

//putInfo completes, serializes and (over)writes the actual node key in the db
func (ntx *nodeTx) putNode(mode os.FileMode) (id uint64, n *node, err error) {
	n = &node{
		Size:    0,
		Mode:    mode,
		ModTime: time.Now(), //@TODO only update if things changed (add checksum)?
	}

	//based on whether the node represents a directory of a file we scan over the chunks or children to update the node struct with up-to-date self information
	if n.Mode.IsDir() {
		if err = ntx.getChildPtrs(func(name string, id uint64) error {
			n.Size = n.Size + 8 //8bytes for each uint64 id
			return nil
		}); err != nil {
			return 0, nil, err
		}

	} else {
		if err = ntx.getChunkPtrs(func(offset int64, k K) error {
			if k == ZeroKey {
				n.Size = offset
			}

			return nil
		}); err != nil {
			return 0, nil, err
		}
	}

	d, err := json.Marshal(n)
	if err != nil {
		return 0, nil, ErrSerialize
	}

	err = ntx.tx.Bucket(NodeBucketName).Put(u64tob(ntx.id), d)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to put node %v: %v", ntx.id, err)
	}

	return ntx.id, n, nil
}

//getNode deserializes the node information and returns it
func (ntx *nodeTx) getNode() (n *node, err error) {
	v := ntx.tx.Bucket(NodeBucketName).Get(u64tob(ntx.id))
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

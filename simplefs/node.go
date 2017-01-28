package simplefs

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

// u64tob converts a uint64 into an 8-capacity byte slice. From the author of bolt on sequential writes: https://github.com/boltdb/bolt/issues/338
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
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

//putChunkPtr writes a prefixed key that points to a content-based chunk key
func (ntx *nodeTx) putChunkPtr(offset int64, k [sha256.Size]byte) (err error) {
	//1. create key using ntx.id and offset
	//2. write content-based chunk key 'k' as value under db key
	return ErrNotImplemented
}

//putChildPtr writes a prefixed key that points to another node
func (ntx *nodeTx) putChildPtr(name string, id uint64) (err error) {
	//1. create key using the ntx.id and child name
	//2. write child id 'id' under db key
	return ErrNotImplemented
}

//putInfo completes, serializes and (over)write the actual node
func (ntx *nodeTx) putNode(mode os.FileMode) (id uint64, n *node, err error) {
	n = &node{
		Size:    0,
		Mode:    mode,       //@TODO infer from presensense of childPtr/chunkPtr?
		ModTime: time.Now(), //@TODO only update if things changed (checksum)?
	}

	//@TODO cursor over node to refresh size data
	//@TODO choose a different serialization method?

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
	//1. get value under key ntx.id
	//2. deserialize and return
	return n, ErrNotImplemented
}

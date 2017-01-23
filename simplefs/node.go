package simplefs

import (
	"crypto/sha256"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

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

//putInfo completes, serializes and writes the actual node
func (ntx *nodeTx) putNode(mode os.FileMode) (n *node, err error) {
	//1. cursor over node to refresh necessary data
	//2. complete node data with and mode, modtime
	//3. serialize node
	//4. write serialized node as value and ntx.id as key
	return n, ErrNotImplemented
}

//getNode deserializes the node information and returns it
func (ntx *nodeTx) getNode() (n *node, err error) {
	//1. get value under key ntx.id
	//2. deserialize and return
	return n, ErrNotImplemented
}

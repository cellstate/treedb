package layerfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/boltdb/bolt"
)

var (
	//ZeroKey is a empty key
	ZeroKey = K{}
)

var (
	//LayerBucketName is the name of the bucket that will hold the layers
	LayerBucketName = []byte("Layer")
	//NodeBucketName is the name of the bucket that will hold the nodes
	NodeBucketName = []byte("Node")
	//ChunkBucketName is the name of the bucket that will hold the chunks
	ChunkBucketName = []byte("Chunk")
)

var (
	//ChunkOffsetSeparator separates a node key from a chunk offset
	ChunkOffsetSeparator = ":"
)

var (
	//ErrSerialize is returned when we couldnt serialize
	ErrSerialize = errors.New("failed to serialize")

	//ErrDeserialize is returned when we couldnt deserialize
	ErrDeserialize = errors.New("failed to deserialize")
)

//LayerFS is an userland, append only, deduplicated filesystem build on top of boltdb
type LayerFS struct {
	layerk K        //key of the current layer
	db     *bolt.DB //the key-value database
}

//K is used as the database key for content addressing
type K [sha256.Size]byte

//New sets up a new filesystem at the specified layer, if the provided layer is a ZeroKey key, writes will be played in a new layer. If the layer is not the latest layer the filesystem will be read-only, else writes will be added to the top layer
func New(layerk K, db *bolt.DB) (fs *LayerFS, err error) {
	fs = &LayerFS{
		layerk: layerk,
		db:     db,
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists(NodeBucketName); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to prepare db: %v", err)
	}

	return fs, nil
}

// u64tob converts a uint64 into an 8-byte slice. From the author of bolt, @see https://github.com/boltdb/bolt/issues/338
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func v64tob(v int64) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(b, v)
	return b
}

//cow will copy-on-write a new node while merging children 'mChildren' and  chunks 'mChunks' with the existing node at key 'nodeK'.
//
//When the node's key 'nodeK' is a ZeroKey a new node is instead created with just the provided children 'mChildren' and chunks 'mChunks'
//
//When the key of a merged child is a ZeroKey the child acts as a tombstone and is removed from the new node instead.
//
//When the first new chunk is mapped to a lower file offset then existing chunks only chunks positioned lower than the new offset are copied over. If the first merged chunk has an offset of zero we are rewriting the node completely, and if then the first chunk has a zerokey we are truncating the node's content.
//
//When succesfull returns a key of the newly created node.
func (fs *LayerFS) cow(
	tx *bolt.Tx,
	node *Node,
	mChildren map[string][]byte,
	mChunks map[int64]K,
) (k []byte, err error) {

	b := tx.Bucket(NodeBucketName)
	nexti, err := b.NextSequence()
	if err != nil {
		return nil, err
	}

	//new node will be at key k
	k = u64tob(nexti)

	//start writing child keys, prefixed with this new keys such that seeks can easily traverse down the tree. Keep tombstones around to filter copied paths
	tombstones := []string{}
	for name, childk := range mChildren {
		if len(childk) == 0 {
			tombstones = append(tombstones, name)
			continue
		}

		if err = b.Put(bytes.Join([][]byte{k, []byte(name)}, []byte(PathSeparator)), childk); err != nil {
			return nil, err
		}
	}

	//@TODO write chunks, make sure ordering based on offset is correct
	//@TODO support truncation, appending and partial differences
	// for offset, chunkk := range mChunks {}
	//@TODO copy over old children, unless tombstones

	//we now read back everything we wrote (all stuff prefixed with key 'k') to compute the node's checksum, boltdb makes sure everything is ordered consistently
	c := b.Cursor()
	h := sha256.New()
	for kk, v := c.Seek(k); kk != nil && bytes.HasPrefix(kk, k); kk, v = c.Next() {

		n, err := h.Write(v)
		if err != nil || n != len(v) {
			return nil, fmt.Errorf("failed to hash new node's content: %v", err)
		}

		//@TODO update the nodes size, handle file sizes by counting chunk offsets
		// node.S = node.S + n

		fmt.Println(kk, v)
	}

	//serialize the node
	data, err := json.Marshal(node)
	if err != nil {
		return nil, ErrSerialize
	}

	//write checksum and data to a buffer
	buf := bytes.NewBuffer(h.Sum(data))
	n, err := buf.Write(data)
	if err != nil || n != len(data) {
		return nil, fmt.Errorf("failed to write serialized to buf: %v", err)
	}

	//finally write the checksummed node
	err = b.Put(k, buf.Bytes())
	if err != nil {
		return nil, err
	}

	return k, nil
}

//getLayer fetches a layer using layer key 'layerk' return os.ErrNotExist if it couldnt be found
// func (fs *LayerFS) getLayer(tx *bolt.Tx, layerk K) (l *Layer, err error) {
// 	data := tx.Bucket(LayerBucketName).Get(layerk[:])
// 	if data == nil {
// 		return nil, os.ErrNotExist
// 	}
//
// 	l = &Layer{}
// 	err = json.Unmarshal(data, &l)
// 	if err != nil {
// 		return nil, ErrDeserialize
// 	}
//
// 	return nil, nil
// }

//putNode updates or inserts node 'n' at path 'p', rehashing all nodes up the tree until finally the current layer key is updated version
// func (fs *LayerFS) putNode(tx *bolt.Tx, n *Node, p P) (err error) {
//
// 	//serialize new node
// 	data, err := json.Marshal(n)
// 	if err != nil {
// 		return ErrSerialize
// 	}
//
// 	//key of the node is its sha checksum
// 	k := sha256.Sum256(data)
// 	err = tx.Bucket(NodeBucketName).Put(k[:], data)
// 	if err != nil {
// 		return err
// 	}
//
// 	//unless this is the root, update the parent
// 	if !p.Equals(Root) {
// 		//we want to update the parent
// 		parent, err := fs.getNode(tx, p.Parent())
// 		if err != nil {
// 			return err //failed to get parent node
// 		}
//
// 		//update the parent with its new key
// 		//@TODO how to cascade the rehashing up the tree
// 		//@TODO how to not load all children keys into memory
// 		err = parent.putChild(tx, k)
// 		if err != nil {
// 			return err //failed to write parent's new child
// 		}
// 	}
//
// 	return nil
// }

//getNode returns the node at path 'p' and return it
//@TODO some redundancy options https://github.com/borgbackup/borg/issues/225
//http://serverfault.com/questions/696216/hard-disk-ssds-detection-and-handling-of-errors-is-silent-data-corruption
//@TODO can we automatically recover from corruption (bit rot)
//@TODO can do brute force redundancy in a (redundant) bolt db
//@TODO can we do autorecover on a read-only database? transaction?
//@TODO we should setup a boltdb abstraction that can automatically recover data from backup interface, configure a local copy, size > N bytes
// func (fs *LayerFS) getNode(tx *bolt.Tx, p P) (n *Node, err error) {
// 	l, err := fs.getLayer(tx, fs.layerk)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return l.Root.getDescendant(tx, p)
// }

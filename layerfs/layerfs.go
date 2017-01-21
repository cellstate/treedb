package layerfs

import (
	"crypto/sha256"

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

	return fs, nil
}

//putNode updates or inserts node 'n' at path 'p', rehashing all nodes up the tree until finally the current layer key is updated version
func (fs *LayerFS) putNode(tx bolt.Tx, n *Node, p P) (err error) {

	return nil
}

//getNode returns the node at path 'p' and return it
func (fs *LayerFS) getNode(tx bolt.Tx, p P) (n *Node, err error) {

	return nil, nil
}

//A Layer represents a point-in-time snapshot of a node tree with file chunks. The fileystem is always created with a specific "top" layer to which new data can be written.
type Layer struct {
	Root *Node //the top node
}

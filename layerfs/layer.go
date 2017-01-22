package layerfs

//A Layer represents a point-in-time snapshot of a node tree with file chunks. The fileystem is always created with a specific "top" layer to which new data can be written.
type Layer struct {
	Root *Node //the top node
}

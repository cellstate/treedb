package simplefs

import "crypto/sha256"

var (
	//FileBucketName is the name of the bucket that will hold files
	FileBucketName = []byte("files")
)

//K is a content-based key
type K [sha256.Size]byte

var (
	//ZeroKey is an content key with only 0x00 bytes
	ZeroKey = K{}
)

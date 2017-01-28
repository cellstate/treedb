package simplefs

import "crypto/sha256"

//K is a content-based key
type K [sha256.Size]byte

var (
	//ZeroKey is an content key with only 0x00 bytes
	ZeroKey = K{}
)

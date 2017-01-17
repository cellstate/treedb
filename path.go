package treedb

import (
	"errors"
	"os"
	"strings"
)

const (
	//PathSeparator is used to join path components and is equal across platform
	//making the database to portable
	PathSeparator = "/"
)

var (
	// ErrInvalidPath is returned when no valid filename can be created from path components
	ErrInvalidPath = errors.New("invalid path components")
)

//P describes a platform agnostic path on the file system and is stored as
//a slice of path components
type P []string

var (
	//Root is a path with zero components: len(Root) = 0
	Root = P{}
)

//Validate is used to check if a given Path is valid, it
//returns an ErrInvalidPath if the path is invalid nil otherwise
func (p P) Validate() error {
	for _, c := range p {
		if strings.Contains(c, PathSeparator) {
			return ErrInvalidPath
		}
	}

	return nil
}

//Parent returns a path that refers to a parent, if the current
//path is the root the root is still returned
func (p P) Parent() P {
	if len(p) < 2 {
		return Root
	}

	return p[:len(p)-1]
}

//Base returns the base component of a path
func (p P) Base() string {
	if len(p) < 1 {
		return PathSeparator
	}

	return p[len(p)-1]
}

//Key returns a byte slice used for database retrieval and storage
func (p P) Key() []byte {
	return []byte(p.String())
}

//String implements stringer for the Path type
func (p P) String() string {
	return PathSeparator + strings.Join(p, PathSeparator)
}

//Err allows easy creation of PathErrors
func (p P) Err(op string, err error) *os.PathError {
	return &os.PathError{Op: op, Err: err, Path: p.String()}
}

package simplefs

import (
	"os"
	"strings"
)

const (
	//PathSeparator is used to join path into database keys. Bolt stores values in a bucket in byte-order, choosing a unicode code point all the way at the end allows us to make assumptions when we use a cursor to iterate over directory entries
	PathSeparator = "\uFFFF"

	//PathPrintSeparator is used instead of the character above to print a path
	PathPrintSeparator = "/"

	//RootBasename is returned when the root is asked for its basename
	RootBasename = PathSeparator
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

//Equals compare paths based on their joined components
func (p P) Equals(d P) bool {
	return (strings.Join(p, PathSeparator) == strings.Join(d, PathSeparator))
}

//String implements stringer for the Path type that returns something more human friendly that shows familiar forward slashes
func (p P) String() string {
	return PathPrintSeparator + strings.Join(p, PathPrintSeparator)
}

//Err allows easy creation of PathErrors
func (p P) Err(op string, err error) *os.PathError {
	return &os.PathError{Op: op, Err: err, Path: p.String()}
}

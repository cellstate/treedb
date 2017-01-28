package simplefs

import "errors"

var (
	//ErrNotImplemented is returned when a piece of functionality is not yet implemented
	ErrNotImplemented = errors.New("not implemented")
	// ErrInvalidPath is returned when no valid filename can be created from path components
	ErrInvalidPath = errors.New("invalid path components")
)

var (
	//ErrNotDirectory is returned when a directory was expected
	ErrNotDirectory = errors.New("not a directory")
	//ErrNotEmptyDirectory tells us the directory was not empty
	ErrNotEmptyDirectory = errors.New("directory is not empty")
)

var (
	//ErrSerialize is returned when we couldnt serialize
	ErrSerialize = errors.New("failed to serialize")

	//ErrDeserialize is returned when we couldnt deserialize
	ErrDeserialize = errors.New("failed to deserialize")
)

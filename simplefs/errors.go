package simplefs

import "errors"

var (
	//ErrNotImplemented is returned when a piece of functionality is not yet implemented
	ErrNotImplemented = errors.New("not implemented")
	// ErrInvalidPath is returned when no valid filename can be created from path components
	ErrInvalidPath = errors.New("invalid path components")
)

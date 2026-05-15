package storage

import "github.com/spf13/afero"

// NewMemoryStorage creates a storage rooted in an in-memory afero filesystem.
func NewMemoryStorage() Storage {
	return newStorage(afero.NewMemMapFs())
}

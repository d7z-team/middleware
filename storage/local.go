package storage

import (
	"os"

	"github.com/spf13/afero"
)

// NewLocalStorage creates a storage rooted at the given local filesystem path.
//
// The root directory is created automatically when it does not already exist.
func NewLocalStorage(root string) (Storage, error) {
	return newLocalStorage(root)
}

func newLocalStorage(root string) (Storage, error) {
	if root == "" {
		return nil, ErrInvalidPath
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return newStorage(afero.NewBasePathFs(afero.NewOsFs(), root)), nil
}

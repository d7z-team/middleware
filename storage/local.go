package storage

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"
	"gopkg.d7z.net/middleware/utils"
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
	opened, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	return newStorage(&localRootFS{root: opened, name: root}), nil
}

type localRootFS struct {
	root *os.Root
	name string
}

func (fs *localRootFS) rootName(op, name string) (string, error) {
	rel, err := utils.NormalizePath(name)
	if err != nil {
		return "", &os.PathError{Op: op, Path: name, Err: ErrInvalidPath}
	}
	if rel == "" {
		return ".", nil
	}
	return filepath.FromSlash(rel), nil
}

func (fs *localRootFS) Create(name string) (afero.File, error) {
	name, err := fs.rootName("create", name)
	if err != nil {
		return nil, err
	}
	return fs.root.Create(name)
}

func (fs *localRootFS) Mkdir(name string, perm os.FileMode) error {
	name, err := fs.rootName("mkdir", name)
	if err != nil {
		return err
	}
	return fs.root.Mkdir(name, perm)
}

func (fs *localRootFS) MkdirAll(name string, perm os.FileMode) error {
	name, err := fs.rootName("mkdir", name)
	if err != nil {
		return err
	}
	return fs.root.MkdirAll(name, perm)
}

func (fs *localRootFS) Open(name string) (afero.File, error) {
	name, err := fs.rootName("open", name)
	if err != nil {
		return nil, err
	}
	return fs.root.Open(name)
}

func (fs *localRootFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	name, err := fs.rootName("openfile", name)
	if err != nil {
		return nil, err
	}
	return fs.root.OpenFile(name, flag, perm)
}

func (fs *localRootFS) Remove(name string) error {
	name, err := fs.rootName("remove", name)
	if err != nil {
		return err
	}
	return fs.root.Remove(name)
}

func (fs *localRootFS) RemoveAll(name string) error {
	name, err := fs.rootName("remove", name)
	if err != nil {
		return err
	}
	if name != "." {
		return fs.root.RemoveAll(name)
	}

	root, err := fs.root.Open(".")
	if err != nil {
		return err
	}
	defer root.Close()

	names, err := root.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, child := range names {
		if err := fs.root.RemoveAll(child); err != nil {
			return err
		}
	}
	return nil
}

func (fs *localRootFS) Rename(oldname, newname string) error {
	oldname, err := fs.rootName("rename", oldname)
	if err != nil {
		return err
	}
	newname, err = fs.rootName("rename", newname)
	if err != nil {
		return err
	}
	if oldname == "." || newname == "." {
		return &os.PathError{Op: "rename", Path: oldname, Err: os.ErrInvalid}
	}
	return fs.root.Rename(oldname, newname)
}

func (fs *localRootFS) Stat(name string) (os.FileInfo, error) {
	name, err := fs.rootName("stat", name)
	if err != nil {
		return nil, err
	}
	return fs.root.Stat(name)
}

func (fs *localRootFS) Name() string { return fs.name }

func (fs *localRootFS) Chmod(name string, mode os.FileMode) error {
	name, err := fs.rootName("chmod", name)
	if err != nil {
		return err
	}
	return fs.root.Chmod(name, mode)
}

func (fs *localRootFS) Chown(name string, uid, gid int) error {
	name, err := fs.rootName("chown", name)
	if err != nil {
		return err
	}
	return fs.root.Chown(name, uid, gid)
}

func (fs *localRootFS) Chtimes(name string, atime, mtime time.Time) error {
	name, err := fs.rootName("chtimes", name)
	if err != nil {
		return err
	}
	return fs.root.Chtimes(name, atime, mtime)
}

func (fs *localRootFS) Close() error {
	if fs == nil || fs.root == nil {
		return nil
	}
	return fs.root.Close()
}

var (
	_ afero.Fs  = (*localRootFS)(nil)
	_ io.Closer = (*localRootFS)(nil)
)

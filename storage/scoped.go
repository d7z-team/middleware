package storage

import (
	"os"
	"path"
	"time"

	"github.com/spf13/afero"
	"gopkg.d7z.net/middleware/utils"
)

type scopedFS struct {
	base   afero.Fs
	prefix string
}

func (s *scopedFS) scopedName(op, name string) (string, error) {
	rel, err := utils.NormalizePath(name)
	if err != nil {
		return "", &os.PathError{Op: op, Path: name, Err: ErrInvalidPath}
	}
	if rel == "" {
		return s.prefix, nil
	}
	return path.Join(s.prefix, rel), nil
}

func (s *scopedFS) Create(name string) (afero.File, error) {
	name, err := s.scopedName("create", name)
	if err != nil {
		return nil, err
	}
	return s.base.Create(name)
}

func (s *scopedFS) Mkdir(name string, perm os.FileMode) error {
	name, err := s.scopedName("mkdir", name)
	if err != nil {
		return err
	}
	return s.base.Mkdir(name, perm)
}

func (s *scopedFS) MkdirAll(name string, perm os.FileMode) error {
	name, err := s.scopedName("mkdir", name)
	if err != nil {
		return err
	}
	return s.base.MkdirAll(name, perm)
}

func (s *scopedFS) Open(name string) (afero.File, error) {
	name, err := s.scopedName("open", name)
	if err != nil {
		return nil, err
	}
	return s.base.Open(name)
}

func (s *scopedFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	name, err := s.scopedName("openfile", name)
	if err != nil {
		return nil, err
	}
	return s.base.OpenFile(name, flag, perm)
}

func (s *scopedFS) Remove(name string) error {
	name, err := s.scopedName("remove", name)
	if err != nil {
		return err
	}
	return s.base.Remove(name)
}

func (s *scopedFS) RemoveAll(name string) error {
	name, err := s.scopedName("remove", name)
	if err != nil {
		return err
	}
	return s.base.RemoveAll(name)
}

func (s *scopedFS) Rename(oldname, newname string) error {
	oldname, err := s.scopedName("rename", oldname)
	if err != nil {
		return err
	}
	newname, err = s.scopedName("rename", newname)
	if err != nil {
		return err
	}
	return s.base.Rename(oldname, newname)
}

func (s *scopedFS) Stat(name string) (os.FileInfo, error) {
	name, err := s.scopedName("stat", name)
	if err != nil {
		return nil, err
	}
	return s.base.Stat(name)
}

func (s *scopedFS) Name() string { return s.base.Name() }

func (s *scopedFS) Chmod(name string, mode os.FileMode) error {
	name, err := s.scopedName("chmod", name)
	if err != nil {
		return err
	}
	return s.base.Chmod(name, mode)
}

func (s *scopedFS) Chown(name string, uid, gid int) error {
	name, err := s.scopedName("chown", name)
	if err != nil {
		return err
	}
	return s.base.Chown(name, uid, gid)
}

func (s *scopedFS) Chtimes(name string, atime, mtime time.Time) error {
	name, err := s.scopedName("chtimes", name)
	if err != nil {
		return err
	}
	return s.base.Chtimes(name, atime, mtime)
}

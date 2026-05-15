package storage

import (
	"cmp"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/afero"
)

// Mount describes one mounted backend inside a MountFS view.
type Mount struct {
	Prefix string
	Fs     afero.Fs
}

// MountFS exposes a merged filesystem view with mounted subtrees.
//
// The default filesystem owns the root view, while mounted filesystems replace
// specific subpaths. Mounted paths and virtual intermediate directories are
// treated as structural boundaries and cannot be renamed or removed as ordinary
// directories.
type MountFS struct {
	mounts    []Mount
	defaultFs afero.Fs
	mu        sync.RWMutex
}

type mountPathKind int

const (
	mountPathMissing mountPathKind = iota
	mountPathReal
	mountPathMount
	mountPathVirtual
)

var errContainsMount = errors.New("directory contains a mount point")

// NewMountFS creates a merged filesystem rooted at defaultFs.
//
// When defaultFs is nil, a host-backed afero.OsFs is used.
func NewMountFS(defaultFs afero.Fs) *MountFS {
	if defaultFs == nil {
		defaultFs = afero.NewOsFs()
	}
	return &MountFS{
		mounts:    make([]Mount, 0),
		defaultFs: defaultFs,
	}
}

// NormalizeMountPath converts a user path into a cleaned absolute mount path.
func NormalizeMountPath(p string) string {
	p = path.Clean(filepath.ToSlash(p))
	if p == "." {
		p = "/"
	}
	return "/" + strings.Trim(p, "/")
}

// Mount attaches backend at the given absolute prefix.
func (m *MountFS) Mount(prefix string, backend afero.Fs) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix = NormalizeMountPath(prefix)
	if prefix == "/" {
		return &os.PathError{Op: "mount", Path: prefix, Err: fs.ErrInvalid}
	}
	if backend == nil {
		return &os.PathError{Op: "mount", Path: prefix, Err: fs.ErrInvalid}
	}
	for _, mount := range m.mounts {
		if mount.Prefix == prefix {
			return &os.PathError{Op: "mount", Path: prefix, Err: os.ErrExist}
		}
	}

	m.mounts = append(m.mounts, Mount{Prefix: prefix, Fs: backend})
	slices.SortFunc(m.mounts, func(a, b Mount) int {
		return -cmp.Compare(a.Prefix, b.Prefix)
	})
	return nil
}

// Unmount removes the mounted backend at the given prefix.
func (m *MountFS) Unmount(prefix string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	prefix = NormalizeMountPath(prefix)
	for i, mount := range m.mounts {
		if mount.Prefix == prefix {
			m.mounts = append(m.mounts[:i], m.mounts[i+1:]...)
			return true
		}
	}
	return false
}

// GetMount returns the backend responsible for name and the backend-relative path.
func (m *MountFS) GetMount(name string) (afero.Fs, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = NormalizeMountPath(name)
	if name == "/" {
		return m.defaultFs, name
	}
	for _, mount := range m.mounts {
		if name == mount.Prefix || strings.HasPrefix(name, mount.Prefix+"/") {
			rel := strings.TrimPrefix(name, mount.Prefix)
			if rel == "" {
				rel = "/"
			}
			return mount.Fs, rel
		}
	}
	return m.defaultFs, name
}

func (m *MountFS) Create(name string) (afero.File, error) {
	mount, rel := m.GetMount(name)
	return mount.Create(rel)
}

func (m *MountFS) Mkdir(name string, perm os.FileMode) error {
	name = NormalizeMountPath(name)
	if _, kind, err := m.mergedStat(name); err == nil && kind != mountPathMissing {
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	}

	mount, rel := m.GetMount(name)
	return mount.Mkdir(rel, perm)
}

func (m *MountFS) MkdirAll(name string, perm os.FileMode) error {
	name = NormalizeMountPath(name)
	if info, kind, err := m.mergedStat(name); err == nil {
		if info != nil && info.IsDir() && (kind == mountPathReal || kind == mountPathMount || kind == mountPathVirtual) {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	}

	mount, rel := m.GetMount(name)
	return mount.MkdirAll(rel, perm)
}

func (m *MountFS) Remove(name string) error {
	name = NormalizeMountPath(name)
	if err := m.rejectStructuralChange("remove", name); err != nil {
		return err
	}

	mount, rel := m.GetMount(name)
	return mount.Remove(rel)
}

func (m *MountFS) RemoveAll(name string) error {
	name = NormalizeMountPath(name)
	if err := m.rejectStructuralChange("remove", name); err != nil {
		return err
	}

	mount, rel := m.GetMount(name)
	return mount.RemoveAll(rel)
}

func (m *MountFS) Rename(oldname, newname string) error {
	oldname = NormalizeMountPath(oldname)
	newname = NormalizeMountPath(newname)

	srcInfo, srcKind, err := m.mergedStat(oldname)
	if err != nil {
		return err
	}
	if srcKind == mountPathMount || srcKind == mountPathVirtual {
		return &os.PathError{Op: "rename", Path: oldname, Err: os.ErrPermission}
	}
	if m.hasChildMount(oldname) {
		return &os.PathError{Op: "rename", Path: oldname, Err: errContainsMount}
	}

	if _, dstKind, err := m.mergedStat(newname); err == nil {
		if dstKind == mountPathMount || dstKind == mountPathVirtual {
			return &os.PathError{Op: "rename", Path: newname, Err: os.ErrPermission}
		}
		return &os.PathError{Op: "rename", Path: newname, Err: os.ErrExist}
	}
	if m.hasChildMount(newname) {
		return &os.PathError{Op: "rename", Path: newname, Err: errContainsMount}
	}

	oldFs, oldPath := m.GetMount(oldname)
	newFs, newPath := m.GetMount(newname)
	if oldFs == newFs {
		return oldFs.Rename(oldPath, newPath)
	}
	return m.crossRename(oldFs, oldPath, srcInfo, newFs, newPath)
}

func (m *MountFS) crossRename(srcFs afero.Fs, src string, srcInfo os.FileInfo, dstFs afero.Fs, dst string) error {
	parent := path.Dir(dst)
	parentInfo, err := dstFs.Stat(parent)
	if err != nil {
		return err
	}
	if !parentInfo.IsDir() {
		return &os.PathError{Op: "rename", Path: dst, Err: fs.ErrInvalid}
	}

	if srcInfo.IsDir() {
		if err := dstFs.Mkdir(dst, srcInfo.Mode()); err != nil {
			return err
		}
		if err := copyDirTree(srcFs, src, dstFs, dst); err != nil {
			_ = dstFs.RemoveAll(dst)
			return err
		}
		return srcFs.RemoveAll(src)
	}

	if err := copyMountedFile(srcFs, src, srcInfo, dstFs, dst); err != nil {
		return err
	}
	return srcFs.Remove(src)
}

func copyDirTree(srcFs afero.Fs, src string, dstFs afero.Fs, dst string) error {
	dir, err := srcFs.Open(src)
	if err != nil {
		return err
	}
	defer dir.Close()

	infos, err := dir.Readdir(-1)
	if err != nil {
		return err
	}
	for _, info := range infos {
		srcPath := path.Join(src, info.Name())
		dstPath := path.Join(dst, info.Name())

		if info.IsDir() {
			if err := dstFs.Mkdir(dstPath, info.Mode()); err != nil {
				return err
			}
			if err := copyDirTree(srcFs, srcPath, dstFs, dstPath); err != nil {
				return err
			}
			continue
		}
		if err := copyMountedFile(srcFs, srcPath, info, dstFs, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func copyMountedFile(srcFs afero.Fs, src string, srcInfo os.FileInfo, dstFs afero.Fs, dst string) error {
	srcFile, err := srcFs.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := dstFs.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFs.Remove(dst)
		return err
	}
	if err := dstFs.Chmod(dst, srcInfo.Mode()); err != nil {
		_ = dstFs.Remove(dst)
		return err
	}
	return nil
}

// Stat returns file information from the merged view.
func (m *MountFS) Stat(name string) (os.FileInfo, error) {
	info, _, err := m.mergedStat(name)
	return info, err
}

// Name returns the filesystem name reported to afero callers.
func (m *MountFS) Name() string {
	return "MountFS"
}

func (m *MountFS) Chmod(name string, mode os.FileMode) error {
	mount, rel := m.GetMount(name)
	return mount.Chmod(rel, mode)
}

func (m *MountFS) Chown(name string, uid, gid int) error {
	mount, rel := m.GetMount(name)
	return mount.Chown(rel, uid, gid)
}

func (m *MountFS) Chtimes(name string, atime, mtime time.Time) error {
	mount, rel := m.GetMount(name)
	return mount.Chtimes(rel, atime, mtime)
}

func (m *MountFS) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	info, kind, err := m.mergedStat(name)
	if err == nil && (kind == mountPathMount || kind == mountPathVirtual) {
		return info, false, nil
	}

	mount, rel := m.GetMount(name)
	if lstater, ok := mount.(afero.Lstater); ok {
		return lstater.LstatIfPossible(rel)
	}
	info, err = mount.Stat(rel)
	return info, false, err
}

func (m *MountFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	mount, rel := m.GetMount(name)
	file, err := mount.OpenFile(rel, flag, perm)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.IsDir() {
		return file, nil
	}

	wrapped, err := newMountFSFile(file, m, name)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return wrapped, nil
}

func (m *MountFS) Open(name string) (afero.File, error) {
	name = NormalizeMountPath(name)

	info, kind, err := m.mergedStat(name)
	if err == nil && kind == mountPathVirtual && info.IsDir() {
		memFs := afero.NewMemMapFs()
		virtualDir, err := memFs.OpenFile("/", os.O_RDONLY, 0)
		if err != nil {
			return nil, err
		}
		wrapped, err := newMountFSFile(virtualDir, m, name)
		if err != nil {
			_ = virtualDir.Close()
			return nil, err
		}
		return wrapped, nil
	}

	return m.OpenFile(name, os.O_RDONLY, 0)
}

func (m *MountFS) SymlinkIfPossible(oldname, newname string) error {
	oldFs, oldPath := m.GetMount(oldname)
	newFs, newPath := m.GetMount(newname)

	if oldFs != newFs {
		return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: fs.ErrInvalid}
	}
	if linker, ok := oldFs.(afero.Linker); ok {
		return linker.SymlinkIfPossible(oldPath, newPath)
	}
	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: afero.ErrNoSymlink}
}

func (m *MountFS) ReadlinkIfPossible(name string) (string, error) {
	mount, rel := m.GetMount(name)
	if linker, ok := mount.(afero.LinkReader); ok {
		return linker.ReadlinkIfPossible(rel)
	}
	return "", &os.PathError{Op: "readlink", Path: name, Err: afero.ErrNoReadlink}
}

// ListMounts returns a snapshot of the currently registered mount points.
func (m *MountFS) ListMounts() []Mount {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Mount, len(m.mounts))
	copy(out, m.mounts)
	return out
}

// GetMountInfo returns the matched mount prefix, backend, and backend-relative path.
func (m *MountFS) GetMountInfo(name string) (string, afero.Fs, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = NormalizeMountPath(name)
	for _, mount := range m.mounts {
		if name == mount.Prefix || strings.HasPrefix(name, mount.Prefix+"/") {
			rel := strings.TrimPrefix(name, mount.Prefix)
			if rel == "" {
				rel = "/"
			}
			return mount.Prefix, mount.Fs, rel
		}
	}
	return "/", m.defaultFs, name
}

func (m *MountFS) mergedStat(name string) (os.FileInfo, mountPathKind, error) {
	name = NormalizeMountPath(name)

	if mount, ok := m.directMount(name); ok {
		return &mountFileInfo{
			name:  filepath.Base(name),
			mode:  os.ModeDir | 0o755,
			mount: &mount,
		}, mountPathMount, nil
	}

	mount, rel := m.GetMount(name)
	info, err := mount.Stat(rel)
	if err == nil {
		return info, mountPathReal, nil
	}
	if !os.IsNotExist(err) {
		return nil, mountPathMissing, err
	}

	if m.isVirtualDir(name) {
		return &virtualFileInfo{
			name: filepath.Base(name),
			mode: os.ModeDir | 0o755,
		}, mountPathVirtual, nil
	}
	return nil, mountPathMissing, err
}

func (m *MountFS) rejectStructuralChange(op, name string) error {
	if _, kind, err := m.mergedStat(name); err == nil {
		if kind == mountPathMount || kind == mountPathVirtual {
			return &os.PathError{Op: op, Path: name, Err: os.ErrPermission}
		}
	}
	if m.hasChildMount(name) {
		return &os.PathError{Op: op, Path: name, Err: errContainsMount}
	}
	return nil
}

func (m *MountFS) directMount(name string) (Mount, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name = NormalizeMountPath(name)
	for _, mount := range m.mounts {
		if mount.Prefix == name {
			return mount, true
		}
	}
	return Mount{}, false
}

func (m *MountFS) hasChildMount(name string) bool {
	name = NormalizeMountPath(name)
	prefix := name + "/"
	for _, mount := range m.ListMounts() {
		if strings.HasPrefix(mount.Prefix, prefix) {
			return true
		}
	}
	return false
}

func (m *MountFS) isVirtualDir(name string) bool {
	name = NormalizeMountPath(name)
	for _, mount := range m.ListMounts() {
		if mount.Prefix != name && strings.HasPrefix(mount.Prefix, name+"/") {
			return true
		}
	}
	return false
}

func (m *MountFS) getMountsUnder(name string) []Mount {
	name = NormalizeMountPath(name)
	mounts := m.ListMounts()
	out := make([]Mount, 0, len(mounts))
	for _, mount := range mounts {
		if mount.Prefix == name {
			continue
		}
		if name == "/" || strings.HasPrefix(mount.Prefix, name+"/") {
			out = append(out, mount)
		}
	}
	return out
}

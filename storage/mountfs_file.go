package storage

import (
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/spf13/afero"
)

type mountFSFile struct {
	afero.File
	fs      *MountFS
	path    string
	offset  int
	entries []fs.DirEntry
}

func newMountFSFile(file afero.File, fsys *MountFS, name string) (*mountFSFile, error) {
	f := &mountFSFile{
		File: file,
		fs:   fsys,
		path: NormalizeMountPath(name),
	}

	entries, err := f.collectEntries()
	if err != nil {
		return nil, err
	}
	f.entries = entries
	return f, nil
}

func (f *mountFSFile) Readdir(count int) ([]os.FileInfo, error) {
	if f.offset >= len(f.entries) {
		if count <= 0 {
			return []os.FileInfo{}, nil
		}
		return nil, io.EOF
	}

	start := f.offset
	end := len(f.entries)
	if count > 0 && start+count < end {
		end = start + count
	}

	infos := make([]os.FileInfo, end-start)
	for i, entry := range f.entries[start:end] {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos[i] = info
	}

	f.offset = end
	return infos, nil
}

func (f *mountFSFile) Readdirnames(count int) ([]string, error) {
	if f.offset >= len(f.entries) {
		if count <= 0 {
			return []string{}, nil
		}
		return nil, io.EOF
	}

	start := f.offset
	end := len(f.entries)
	if count > 0 && start+count < end {
		end = start + count
	}

	names := make([]string, end-start)
	for i, entry := range f.entries[start:end] {
		names[i] = entry.Name()
	}

	f.offset = end
	return names, nil
}

func (f *mountFSFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		f.offset = 0
	}
	return f.File.Seek(offset, whence)
}

func (f *mountFSFile) collectEntries() ([]fs.DirEntry, error) {
	rawInfos, err := f.File.Readdir(-1)
	if err != nil {
		return nil, err
	}

	entryMap := make(map[string]fs.DirEntry, len(rawInfos))
	for _, info := range rawInfos {
		entryMap[info.Name()] = &dirEntry{info: info}
	}

	for _, mount := range f.fs.getMountsUnder(f.path) {
		relPath := strings.TrimPrefix(mount.Prefix, f.path)
		relPath = strings.TrimPrefix(relPath, "/")
		parts := strings.Split(relPath, "/")
		if len(parts) == 0 {
			continue
		}

		name := parts[0]
		if len(parts) == 1 {
			entryMap[name] = &mountDirEntry{
				name:  name,
				mode:  os.ModeDir | 0o755,
				mount: &mount,
			}
			continue
		}

		if _, exists := entryMap[name]; !exists {
			entryMap[name] = &dirEntry{info: &virtualFileInfo{
				name: name,
				mode: os.ModeDir | 0o755,
			}}
		}
	}

	entries := make([]fs.DirEntry, 0, len(entryMap))
	for _, entry := range entryMap {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries, nil
}

type virtualFileInfo struct {
	name string
	mode os.FileMode
}

func (v *virtualFileInfo) Name() string       { return v.name }
func (v *virtualFileInfo) Size() int64        { return 0 }
func (v *virtualFileInfo) Mode() os.FileMode  { return v.mode }
func (v *virtualFileInfo) ModTime() time.Time { return time.Time{} }
func (v *virtualFileInfo) IsDir() bool        { return v.mode.IsDir() }
func (v *virtualFileInfo) Sys() interface{}   { return nil }

type dirEntry struct {
	info os.FileInfo
}

func (d *dirEntry) Name() string               { return d.info.Name() }
func (d *dirEntry) IsDir() bool                { return d.info.IsDir() }
func (d *dirEntry) Type() fs.FileMode          { return d.info.Mode().Type() }
func (d *dirEntry) Info() (os.FileInfo, error) { return d.info, nil }

type mountDirEntry struct {
	name  string
	mode  os.FileMode
	mount *Mount
}

func (m *mountDirEntry) Name() string               { return m.name }
func (m *mountDirEntry) IsDir() bool                { return m.mode.IsDir() }
func (m *mountDirEntry) Type() fs.FileMode          { return m.mode.Type() }
func (m *mountDirEntry) Info() (os.FileInfo, error) { return m, nil }
func (m *mountDirEntry) Size() int64                { return 0 }
func (m *mountDirEntry) Mode() os.FileMode          { return m.mode }
func (m *mountDirEntry) ModTime() time.Time {
	if m.mount != nil {
		if info, err := m.mount.Fs.Stat("/"); err == nil {
			return info.ModTime()
		}
	}
	return time.Time{}
}
func (m *mountDirEntry) Sys() interface{} { return nil }

type mountFileInfo struct {
	name  string
	mode  os.FileMode
	mount *Mount
}

func (m *mountFileInfo) Name() string { return m.name }
func (m *mountFileInfo) Size() int64  { return 0 }
func (m *mountFileInfo) Mode() os.FileMode {
	if m.mount != nil {
		if info, err := m.mount.Fs.Stat("/"); err == nil {
			return info.Mode()
		}
	}
	return m.mode
}

func (m *mountFileInfo) ModTime() time.Time {
	if m.mount != nil {
		if info, err := m.mount.Fs.Stat("/"); err == nil {
			return info.ModTime()
		}
	}
	return time.Time{}
}
func (m *mountFileInfo) IsDir() bool      { return m.mode.IsDir() }
func (m *mountFileInfo) Sys() interface{} { return nil }

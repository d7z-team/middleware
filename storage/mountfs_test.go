package storage

import (
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func newTestMountFS(t *testing.T) (*MountFS, afero.Fs, afero.Fs) {
	t.Helper()

	defaultFs := afero.NewMemMapFs()
	mountedFs := afero.NewMemMapFs()
	mountFS := NewMountFS(defaultFs)
	require.NoError(t, mountFS.Mount("/mounted", mountedFs))
	return mountFS, defaultFs, mountedFs
}

func TestMountFSMountAndGetMount(t *testing.T) {
	mountFS, defaultFs, mountedFs := newTestMountFS(t)

	gotFs, gotPath := mountFS.GetMount("/mounted/file.txt")
	require.Equal(t, mountedFs, gotFs)
	require.Equal(t, "/file.txt", gotPath)

	gotFs, gotPath = mountFS.GetMount("/other/file.txt")
	require.Equal(t, defaultFs, gotFs)
	require.Equal(t, "/other/file.txt", gotPath)
}

func TestMountFSStatIncludesMountAndVirtualDirs(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	mountFS := NewMountFS(defaultFs)
	require.NoError(t, mountFS.Mount("/path/to/alice", afero.NewMemMapFs()))

	for _, name := range []string{"/path", "/path/to", "/path/to/alice"} {
		info, err := mountFS.Stat(name)
		require.NoError(t, err)
		require.True(t, info.IsDir())
	}
}

func TestMountFSOpenMergesEntries(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	require.NoError(t, defaultFs.Mkdir("/dir_in_default", 0o755))
	_, err := defaultFs.Create("/file_in_default.txt")
	require.NoError(t, err)

	mountFS := NewMountFS(defaultFs)
	mountedFs := afero.NewMemMapFs()
	require.NoError(t, mountedFs.Mkdir("/dir_in_mounted", 0o755))
	_, err = mountedFs.Create("/file_in_mounted.txt")
	require.NoError(t, err)
	require.NoError(t, mountFS.Mount("/mount1", mountedFs))
	require.NoError(t, mountFS.Mount("/mount1/sub_mount", afero.NewMemMapFs()))
	require.NoError(t, mountFS.Mount("/mount2", afero.NewMemMapFs()))

	root, err := mountFS.Open("/")
	require.NoError(t, err)
	rootNames, err := root.Readdirnames(0)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dir_in_default", "file_in_default.txt", "mount1", "mount2"}, rootNames)

	dir, err := mountFS.Open("/mount1")
	require.NoError(t, err)
	names, err := dir.Readdirnames(0)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"dir_in_mounted", "file_in_mounted.txt", "sub_mount"}, names)
}

func TestMountFSMkdirAndMkdirAll(t *testing.T) {
	mountFS, _, _ := newTestMountFS(t)

	require.NoError(t, mountFS.Mkdir("/plain", 0o755))
	info, err := mountFS.Stat("/plain")
	require.NoError(t, err)
	require.True(t, info.IsDir())

	err = mountFS.Mkdir("/mounted", 0o755)
	require.Error(t, err)
	require.True(t, os.IsExist(err))

	require.NoError(t, mountFS.MkdirAll("/mounted", 0o755))

	virtualFS := NewMountFS(afero.NewMemMapFs())
	require.NoError(t, virtualFS.Mount("/virtual/path/leaf", afero.NewMemMapFs()))
	require.NoError(t, virtualFS.MkdirAll("/virtual/path", 0o755))
}

func TestMountFSRemoveAndRemoveAllRejectStructuralPaths(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	mountFS := NewMountFS(defaultFs)
	require.NoError(t, mountFS.Mount("/mounted", afero.NewMemMapFs()))
	require.NoError(t, mountFS.Mount("/mounted/sub", afero.NewMemMapFs()))

	err := mountFS.Remove("/mounted")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))

	err = mountFS.RemoveAll("/mounted")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))

	err = mountFS.RemoveAll("/mounted/sub")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))
}

func TestMountFSRemoveAllRegularDirectory(t *testing.T) {
	mountFS, defaultFs, _ := newTestMountFS(t)
	require.NoError(t, defaultFs.MkdirAll("/a/b/c", 0o755))

	err := mountFS.RemoveAll("/a")
	require.NoError(t, err)

	_, err = mountFS.Stat("/a")
	require.True(t, os.IsNotExist(err))
}

func TestMountFSRenameWithinAndAcrossBackends(t *testing.T) {
	mountFS, defaultFs, mountedFs := newTestMountFS(t)

	require.NoError(t, afero.WriteFile(defaultFs, "/file1.txt", []byte("data"), 0o644))
	require.NoError(t, defaultFs.MkdirAll("/src/sub", 0o755))
	require.NoError(t, afero.WriteFile(defaultFs, "/src/sub/file.txt", []byte("x"), 0o644))
	require.NoError(t, mountedFs.Mkdir("/dest-parent", 0o755))

	require.NoError(t, mountFS.Rename("/file1.txt", "/file2.txt"))
	_, err := mountFS.Stat("/file2.txt")
	require.NoError(t, err)

	require.NoError(t, mountFS.Rename("/file2.txt", "/mounted/dest-parent/file3.txt"))
	_, err = mountFS.Stat("/mounted/dest-parent/file3.txt")
	require.NoError(t, err)
	_, err = mountFS.Stat("/file2.txt")
	require.True(t, os.IsNotExist(err))

	require.NoError(t, mountFS.Rename("/src", "/mounted/dest-parent/tree"))
	_, err = mountFS.Stat("/mounted/dest-parent/tree/sub/file.txt")
	require.NoError(t, err)
	_, err = mountFS.Stat("/src")
	require.True(t, os.IsNotExist(err))
}

func TestMountFSRenameRejectsBoundaryCases(t *testing.T) {
	mountFS, defaultFs, mountedFs := newTestMountFS(t)

	require.NoError(t, afero.WriteFile(mountedFs, "/root.txt", []byte("m"), 0o644))
	require.NoError(t, afero.WriteFile(defaultFs, "/src.txt", []byte("s"), 0o644))
	require.NoError(t, defaultFs.Mkdir("/dir", 0o755))
	require.NoError(t, mountedFs.MkdirAll("/parent/existing", 0o755))
	require.NoError(t, mountedFs.Mkdir("/parent2", 0o755))

	err := mountFS.Rename("/mounted", "/moved")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))

	err = mountFS.Rename("/src.txt", "/mounted")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))

	err = mountFS.Rename("/src.txt", "/mounted/parent/existing")
	require.Error(t, err)
	require.True(t, os.IsExist(err))

	err = mountFS.Rename("/dir", "/mounted/missing-parent/tree")
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}

func TestMountFSRenameRejectsVirtualAndChildMountCases(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	mountFS := NewMountFS(defaultFs)
	require.NoError(t, defaultFs.Mkdir("/real", 0o755))
	require.NoError(t, mountFS.Mount("/virtual/leaf", afero.NewMemMapFs()))
	require.NoError(t, mountFS.Mount("/real/submount", afero.NewMemMapFs()))

	err := mountFS.Rename("/virtual", "/other")
	require.Error(t, err)
	require.True(t, isPermissionPathError(err))

	err = mountFS.Rename("/real", "/other-real")
	require.Error(t, err)
	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, "rename", pathErr.Op)
	require.Contains(t, pathErr.Err.Error(), "mount point")
}

func TestMountFSOpenFileWrapsDirectories(t *testing.T) {
	mountFS, defaultFs, mountedFs := newTestMountFS(t)
	require.NoError(t, defaultFs.Mkdir("/dir", 0o755))
	require.NoError(t, mountedFs.Mkdir("/subdir", 0o755))

	dir, err := mountFS.OpenFile("/dir", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, ok := dir.(*mountFSFile)
	require.True(t, ok)

	dir, err = mountFS.OpenFile("/mounted/subdir", os.O_RDONLY, 0)
	require.NoError(t, err)
	_, ok = dir.(*mountFSFile)
	require.True(t, ok)
}

func TestMountFSReaddirAndSeek(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	require.NoError(t, defaultFs.Mkdir("/dir1", 0o755))
	_, err := defaultFs.Create("/file1.txt")
	require.NoError(t, err)

	mountFS := NewMountFS(defaultFs)
	require.NoError(t, mountFS.Mount("/mounted", afero.NewMemMapFs()))

	file, err := mountFS.Open("/")
	require.NoError(t, err)
	defer file.Close()

	dir, ok := file.(*mountFSFile)
	require.True(t, ok)

	entries, err := dir.Readdir(0)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	_, err = dir.Seek(0, io.SeekStart)
	require.NoError(t, err)

	for range 3 {
		names, err := dir.Readdirnames(1)
		require.NoError(t, err)
		require.Len(t, names, 1)
	}

	_, err = dir.Readdirnames(1)
	require.ErrorIs(t, err, io.EOF)
}

func TestMountFSUnmount(t *testing.T) {
	mountFS, defaultFs, _ := newTestMountFS(t)

	require.True(t, mountFS.Unmount("/mounted"))
	gotFs, gotPath := mountFS.GetMount("/mounted/file.txt")
	require.Equal(t, defaultFs, gotFs)
	require.Equal(t, "/mounted/file.txt", gotPath)
	require.False(t, mountFS.Unmount("/mounted"))
}

func TestOverlayStorageURL(t *testing.T) {
	rootDir := t.TempDir()
	assetsDir := t.TempDir()

	rootURL := (&url.URL{Scheme: "local", Path: filepath.ToSlash(rootDir)}).String()
	assetsURL := (&url.URL{Scheme: "local", Path: filepath.ToSlash(assetsDir)}).String()

	raw := "overlay://?upperdir=" + url.QueryEscape(rootURL) +
		"&mount=" + url.QueryEscape("/cache::memory://") +
		"&mount=" + url.QueryEscape("/assets::"+assetsURL)

	st, err := NewStorageFromURL(raw)
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, afero.WriteFile(st, "root.txt", []byte("root"), 0o644))
	require.NoError(t, afero.WriteFile(st, "cache/hot.txt", []byte("mem"), 0o644))
	require.NoError(t, afero.WriteFile(st, "assets/logo.txt", []byte("asset"), 0o644))

	data, err := os.ReadFile(filepath.Join(rootDir, "root.txt"))
	require.NoError(t, err)
	require.Equal(t, "root", string(data))

	_, err = os.Stat(filepath.Join(rootDir, "cache", "hot.txt"))
	require.Error(t, err)

	data, err = os.ReadFile(filepath.Join(assetsDir, "logo.txt"))
	require.NoError(t, err)
	require.Equal(t, "asset", string(data))
}

func TestOverlayStorageURLValidation(t *testing.T) {
	_, err := NewStorageFromURL("overlay://")
	require.ErrorIs(t, err, ErrInvalidMountSpec)

	_, err = NewStorageFromURL("overlay://?upperdir=memory://&mount=" + url.QueryEscape("bad"))
	require.ErrorIs(t, err, ErrInvalidMountSpec)

	_, err = NewStorageFromURL("overlay://?upperdir=memory://&mount=" + url.QueryEscape("/::memory://"))
	require.ErrorIs(t, err, ErrInvalidMountSpec)
}

func isPermissionPathError(err error) bool {
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		return false
	}
	return errors.Is(pathErr.Err, os.ErrPermission)
}

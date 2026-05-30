package storage

import (
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestStorageChildPanicsOnInvalidPath(t *testing.T) {
	root := NewMemoryStorage()
	require.Panics(t, func() { _ = root.Child("..") })
	require.Panics(t, func() { _ = root.Child("a//b") })
	require.Panics(t, func() { _ = root.Child("/a") })
	require.Panics(t, func() { _ = root.Child("") })
}

func TestMountFSHelperMethods(t *testing.T) {
	defaultFs := afero.NewMemMapFs()
	mountedFs := afero.NewMemMapFs()
	mountFS := NewMountFS(defaultFs)
	require.NoError(t, mountFS.Mount("/mounted", mountedFs))
	require.NoError(t, mountFS.Mount("/virtual/leaf", afero.NewMemMapFs()))

	require.Equal(t, "MountFS", mountFS.Name())

	created, err := mountFS.Create("/created.txt")
	require.NoError(t, err)
	require.NoError(t, created.Close())

	_, err = mountFS.Stat("/created.txt")
	require.NoError(t, err)

	prefix, backend, rel := mountFS.GetMountInfo("/mounted/file.txt")
	require.Equal(t, "/mounted", prefix)
	require.Equal(t, mountedFs, backend)
	require.Equal(t, "/file.txt", rel)

	info, isLstat, err := mountFS.LstatIfPossible("/mounted")
	require.NoError(t, err)
	require.False(t, isLstat)
	require.True(t, info.IsDir())
	require.Equal(t, "mounted", info.Name())
	require.Zero(t, info.Size())
	require.NotZero(t, info.Mode())
	_ = info.ModTime()
	require.Nil(t, info.Sys())

	createdInfo, isLstat, err := mountFS.LstatIfPossible("/created.txt")
	require.NoError(t, err)
	require.False(t, isLstat)
	require.Equal(t, "created.txt", createdInfo.Name())

	virtualInfo, err := mountFS.Stat("/virtual")
	require.NoError(t, err)
	require.True(t, virtualInfo.IsDir())
	require.Equal(t, "virtual", virtualInfo.Name())
	require.Zero(t, virtualInfo.Size())
	require.NotZero(t, virtualInfo.Mode())
	_ = virtualInfo.ModTime()
	require.Nil(t, virtualInfo.Sys())

	root, err := mountFS.Open("/")
	require.NoError(t, err)
	defer root.Close()

	mountDir, ok := root.(*mountFSFile)
	require.True(t, ok)
	require.NotEmpty(t, mountDir.entries)
	for _, entry := range mountDir.entries {
		_ = entry.IsDir()
		_ = entry.Type()
		entryInfo, err := entry.Info()
		require.NoError(t, err)
		require.NotEmpty(t, entryInfo.Name())
		_ = entryInfo.Mode()
		_ = entryInfo.ModTime()
		_ = entryInfo.Sys()
	}

	virtual, err := mountFS.Open("/virtual")
	require.NoError(t, err)
	require.NoError(t, virtual.Close())

	require.Error(t, mountFS.Chmod("/missing.txt", 0o644))
	require.Error(t, mountFS.Chown("/missing.txt", 0, 0))
	require.Error(t, mountFS.Chtimes("/missing.txt", time.Now(), time.Now()))

	err = mountFS.SymlinkIfPossible("/created.txt", "/mounted/link.txt")
	var linkErr *os.LinkError
	require.ErrorAs(t, err, &linkErr)
	require.ErrorIs(t, err, fs.ErrInvalid)

	_, err = mountFS.ReadlinkIfPossible("/created.txt")
	require.Error(t, err)
}

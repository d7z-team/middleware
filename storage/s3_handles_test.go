package storage

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestS3FileAndDirectoryHandles(t *testing.T) {
	target := loadTestS3Target(t)
	fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "handles"))
	defer fs.Close()

	t.Run("file_handle_semantics", func(t *testing.T) {
		file, err := fs.Create("notes.txt")
		require.NoError(t, err)
		require.Equal(t, "notes.txt", file.Name())

		written, err := file.WriteString("hello")
		require.NoError(t, err)
		require.Equal(t, 5, written)

		written, err = file.WriteAt([]byte("!"), 5)
		require.NoError(t, err)
		require.Equal(t, 1, written)

		_, err = file.Seek(0, io.SeekStart)
		require.NoError(t, err)

		buf := make([]byte, 5)
		read, err := file.ReadAt(buf, 0)
		require.NoError(t, err)
		require.Equal(t, 5, read)
		require.Equal(t, "hello", string(buf))

		_, err = file.Readdir(1)
		require.Error(t, err)
		_, err = file.Readdirnames(1)
		require.Error(t, err)

		info, err := file.Stat()
		require.NoError(t, err)
		require.Equal(t, "notes.txt", info.Name())
		require.NotZero(t, info.Mode())
		_ = info.ModTime()
		require.Nil(t, info.Sys())

		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())
		require.NoError(t, file.Close())

		data, err := afero.ReadFile(fs, "notes.txt")
		require.NoError(t, err)
		require.Equal(t, "hello!", string(data))
	})

	t.Run("directory_handle_semantics", func(t *testing.T) {
		require.NoError(t, fs.Mkdir("docs", 0o755))
		require.ErrorIs(t, fs.Mkdir("docs", 0o755), os.ErrExist)
		require.NoError(t, afero.WriteFile(fs, "docs/entry.txt", []byte("entry"), 0o644))

		dir, err := fs.Open("docs")
		require.NoError(t, err)
		defer dir.Close()

		require.Equal(t, "docs", dir.Name())
		info, err := dir.Stat()
		require.NoError(t, err)
		require.True(t, info.IsDir())

		entries, err := dir.Readdir(1)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, "entry.txt", entries[0].Name())

		_, err = dir.Seek(0, io.SeekStart)
		require.NoError(t, err)

		names, err := dir.Readdirnames(0)
		require.NoError(t, err)
		require.Equal(t, []string{"entry.txt"}, names)

		_, err = dir.Seek(1, io.SeekCurrent)
		require.Error(t, err)
		_, err = dir.Read(make([]byte, 1))
		require.Error(t, err)
		_, err = dir.ReadAt(make([]byte, 1), 0)
		require.Error(t, err)
		_, err = dir.Write([]byte("x"))
		require.Error(t, err)
		_, err = dir.WriteAt([]byte("x"), 0)
		require.Error(t, err)
		require.Error(t, dir.Truncate(0))
		_, err = dir.WriteString("x")
		require.Error(t, err)
		require.NoError(t, dir.Sync())
	})

	t.Run("openfile_validation_and_root_name", func(t *testing.T) {
		_, err := fs.OpenFile("docs", os.O_WRONLY, 0o644)
		require.Error(t, err)

		_, err = fs.OpenFile("missing.txt", os.O_RDONLY, 0o644)
		require.ErrorIs(t, err, os.ErrNotExist)

		_, err = fs.OpenFile("notes.txt", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		require.ErrorIs(t, err, os.ErrExist)

		root, err := fs.Open("/")
		require.NoError(t, err)
		defer root.Close()
		require.Equal(t, target.bucket, root.Name())
	})
}

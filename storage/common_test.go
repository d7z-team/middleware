package storage

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func testStorageContract(t *testing.T, factory func(t *testing.T) CloserStorage) {
	t.Helper()

	t.Run("file_lifecycle", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		require.NoError(t, fs.MkdirAll("docs", 0o755))
		require.NoError(t, afero.WriteFile(fs, "docs/readme.txt", []byte("hello"), 0o644))

		data, err := afero.ReadFile(fs, "docs/readme.txt")
		require.NoError(t, err)
		require.Equal(t, "hello", string(data))

		info, err := fs.Stat("docs/readme.txt")
		require.NoError(t, err)
		require.False(t, info.IsDir())

		entries, err := afero.ReadDir(fs, "docs")
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Equal(t, "readme.txt", entries[0].Name())

		require.NoError(t, fs.Rename("docs/readme.txt", "docs/guide.txt"))
		_, err = fs.Stat("docs/readme.txt")
		require.Error(t, err)

		data, err = afero.ReadFile(fs, "docs/guide.txt")
		require.NoError(t, err)
		require.Equal(t, "hello", string(data))

		require.NoError(t, fs.Remove("docs/guide.txt"))
		_, err = fs.Stat("docs/guide.txt")
		require.Error(t, err)
	})

	t.Run("openfile_append_and_truncate", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		f, err := fs.OpenFile("data.log", os.O_CREATE|os.O_RDWR, 0o644)
		require.NoError(t, err)
		_, err = f.Write([]byte("abc"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		f, err = fs.OpenFile("data.log", os.O_APPEND|os.O_WRONLY, 0o644)
		require.NoError(t, err)
		_, err = f.Write([]byte("def"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		data, err := afero.ReadFile(fs, "data.log")
		require.NoError(t, err)
		require.Equal(t, "abcdef", string(data))

		f, err = fs.OpenFile("data.log", os.O_RDWR, 0o644)
		require.NoError(t, err)
		require.NoError(t, f.Truncate(3))
		require.NoError(t, f.Close())

		data, err = afero.ReadFile(fs, "data.log")
		require.NoError(t, err)
		require.Equal(t, "abc", string(data))
	})

	t.Run("child_namespace", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		child := fs.Child("tenant", "alpha")
		require.NoError(t, child.MkdirAll("nested", 0o755))
		require.NoError(t, afero.WriteFile(child, "nested/file.txt", []byte("scoped"), 0o644))

		data, err := afero.ReadFile(child, "nested/file.txt")
		require.NoError(t, err)
		require.Equal(t, "scoped", string(data))

		parentData, err := afero.ReadFile(fs, "tenant/alpha/nested/file.txt")
		require.NoError(t, err)
		require.Equal(t, "scoped", string(parentData))

		_, err = child.Stat("../escape.txt")
		require.Error(t, err)
	})

	t.Run("nested_child_namespace", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		nested := fs.Child("a").Child("b/c")
		require.NoError(t, nested.MkdirAll(".", 0o755))
		require.NoError(t, afero.WriteFile(nested, "x.txt", []byte("v"), 0o644))

		data, err := afero.ReadFile(fs, "a/b/c/x.txt")
		require.NoError(t, err)
		require.Equal(t, "v", string(data))
	})

	t.Run("remove_all", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		require.NoError(t, fs.MkdirAll("root/sub", 0o755))
		require.NoError(t, afero.WriteFile(fs, "root/a.txt", []byte("a"), 0o644))
		require.NoError(t, afero.WriteFile(fs, "root/sub/b.txt", []byte("b"), 0o644))
		require.NoError(t, fs.RemoveAll("root"))

		_, err := fs.Stat("root")
		require.Error(t, err)
	})

	t.Run("invalid_child_rejected", func(t *testing.T) {
		fs := factory(t)
		defer fs.Close()

		testCases := [][]string{
			{"."},
			{".."},
			{"a", ".."},
			{"a/./b"},
			{"a/../b"},
			{"a\\..\\b"},
		}

		for _, parts := range testCases {
			t.Run(fmt.Sprintf("%q", parts), func(t *testing.T) {
				child := fs.Child(parts...)
				err := child.MkdirAll("blocked", 0o755)
				require.ErrorIs(t, err, ErrInvalidPath)

				_, err = child.Create("x.txt")
				require.ErrorIs(t, err, ErrInvalidPath)
			})
		}
	})
}

func TestNewStorageFromURL(t *testing.T) {
	mem, err := NewStorageFromURL("memory://")
	require.NoError(t, err)
	require.Equal(t, "MemMapFS", mem.Name())
	require.NoError(t, mem.Close())

	mem, err = NewStorageFromURL("mem://")
	require.NoError(t, err)
	require.NoError(t, mem.Close())

	tempDir := t.TempDir()
	localURL := "local://" + filepath.ToSlash(tempDir)
	local, err := NewStorageFromURL(localURL)
	require.NoError(t, err)
	require.NoError(t, local.Close())

	storageURL := "storage://" + filepath.ToSlash(tempDir)
	local, err = NewStorageFromURL(storageURL)
	require.NoError(t, err)
	require.NoError(t, local.Close())

	s3URL := "s3://middleware-storage-test/test-prefix?access_key=minioadmin&secret_key=minioadmin&region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000&path_style=true&disable_ssl=true"
	s3Store, err := NewStorageFromURL(s3URL)
	require.NoError(t, err)
	require.Equal(t, "S3FS", s3Store.Name())
	require.NoError(t, s3Store.Close())

	_, err = NewStorageFromURL("unknown://")
	require.ErrorIs(t, err, ErrUnsupportedScheme)

	_, err = NewStorageFromURL("local://")
	require.ErrorIs(t, err, ErrInvalidPath)

	_, err = NewStorageFromURL("s3:///missing-bucket")
	require.ErrorIs(t, err, ErrInvalidPath)

	_, err = NewStorageFromURL("s3://bucket/path?path_style=maybe")
	require.ErrorIs(t, err, ErrInvalidPath)

	escapedDir := filepath.Join(tempDir, "with space")
	raw := (&url.URL{Scheme: "local", Path: escapedDir}).String()
	local, err = NewStorageFromURL(raw)
	require.NoError(t, err)
	require.NoError(t, local.Close())
}

func TestMemoryStorageContract(t *testing.T) {
	testStorageContract(t, func(t *testing.T) CloserStorage {
		return closerStorage{
			Storage: NewMemoryStorage(),
			closer:  func() error { return nil },
		}
	})
}

func TestLocalStorageContract(t *testing.T) {
	testStorageContract(t, func(t *testing.T) CloserStorage {
		fs, err := NewLocalStorage(t.TempDir())
		require.NoError(t, err)
		return closerStorage{
			Storage: fs,
			closer:  func() error { return nil },
		}
	})
}

func TestS3StorageContract(t *testing.T) {
	target := loadTestS3Target(t)
	testStorageContract(t, func(t *testing.T) CloserStorage {
		return newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "store"))
	})
}

func TestNewLocalStorageRequiresRoot(t *testing.T) {
	_, err := NewLocalStorage("")
	require.ErrorIs(t, err, ErrInvalidPath)
}

func TestInvalidChildPersistsError(t *testing.T) {
	child := NewMemoryStorage().Child("..").Child("safe")

	_, err := child.Open("x")
	require.ErrorIs(t, err, ErrInvalidPath)
}

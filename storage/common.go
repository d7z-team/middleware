// Package storage provides namespaced filesystem backends built on top of afero.
//
// The package exposes a Storage interface that behaves like an afero filesystem
// and adds Child support for creating scoped subtrees without manually joining
// prefixes throughout the call site.
//
// Supported backends include:
//   - memory:// and mem:// for in-memory storage
//   - local:///abs/path and storage:///abs/path for local directories
//   - overlay:// and mount:// for mounted composite views
//   - s3://bucket/root/prefix for object-backed storage
//
// A typical in-memory usage looks like:
//
//	store, _ := NewStorageFromURL("memory://")
//	defer store.Close()
//
//	tenant := store.Child("tenant-a")
//	_ = afero.WriteFile(tenant, "docs/readme.txt", []byte("hello"), 0o644)
//
//	data, _ := afero.ReadFile(store, "tenant-a/docs/readme.txt")
//	_ = data
//
// A local filesystem root can be opened from a URL:
//
//	store, _ := NewStorageFromURL("local:///var/lib/app")
//	defer store.Close()
//
//	_ = store.MkdirAll("cache", 0o755)
//
// Overlay storage can merge a writable root with mounted subtrees:
//
//	store, _ := NewStorageFromURL("overlay://?upperdir=memory://&mount=/assets::memory://")
//	defer store.Close()
//
//	assets := store.Child("assets")
//	_ = afero.WriteFile(assets, "logo.txt", []byte("png"), 0o644)
package storage

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/spf13/afero"
	"gopkg.d7z.net/middleware/utils"
)

// Storage exposes an afero-backed filesystem with Child support.
//
// Child returns a new view rooted under developer-provided path segments.
// Invalid child paths panic because Child must not receive user input.
type Storage interface {
	afero.Fs
	Child(paths ...string) Storage
}

// CloserStorage combines Storage with io.Closer for URL-built backends that may
// own multiple underlying storages.
type CloserStorage interface {
	Storage
	io.Closer
}

type storageFS struct {
	afero.Fs
}

type closerStorage struct {
	Storage
	closer func() error
}

func (c closerStorage) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer()
}

var (
	// ErrInvalidPath indicates that a Child path or local root path is invalid.
	ErrInvalidPath = errors.New("storage: invalid child path")
	// ErrDirectoryNotEmpty indicates that Remove was called on a non-empty directory.
	ErrDirectoryNotEmpty = errors.New("storage: directory not empty")
	// ErrInvalidMountSpec indicates that an overlay mount definition is malformed.
	ErrInvalidMountSpec = errors.New("storage: invalid mount spec")
	// ErrUnsupportedScheme indicates that the storage URL scheme is not supported.
	ErrUnsupportedScheme = errors.New("storage: unsupported scheme")
)

// NewStorageFromURL creates a storage backend from a connection URL.
//
// Supported schemes:
//   - memory:// and mem://
//   - local:///abs/path and storage:///abs/path
//   - overlay:// and mount://
//   - s3://bucket/root/prefix
//
// Overlay URLs use an overlayfs-like root plus mounted subtrees:
//
//	overlay://?upperdir=local:///data/app&mount=/cache::memory://&mount=/assets::local:///srv/assets
//
// Example:
//
//	store, _ := NewStorageFromURL("overlay://?upperdir=memory://&mount=/assets::memory://")
//	defer store.Close()
//
//	avatars := store.Child("avatars")
//	_ = afero.WriteFile(avatars, "user-1.txt", []byte("hello"), 0o644)
//
//	assets := store.Child("assets")
//	_ = afero.WriteFile(assets, "logo.txt", []byte("png"), 0o644)
func NewStorageFromURL(raw string) (CloserStorage, error) {
	parse, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(parse.Scheme) {
	case "memory", "mem":
		return closerStorage{
			Storage: newStorage(afero.NewMemMapFs()),
			closer:  func() error { return nil },
		}, nil
	case "local", "storage":
		root, err := resolveLocalRoot(parse)
		if err != nil {
			return nil, err
		}
		fs, err := newLocalStorage(root)
		if err != nil {
			return nil, err
		}
		return closerStorage{
			Storage: fs,
			closer: func() error {
				if closer, ok := fs.(io.Closer); ok {
					return closer.Close()
				}
				return nil
			},
		}, nil
	case "overlay", "mount":
		return newOverlayStorageFromURL(parse)
	case "s3":
		fs, err := newS3StorageFromURL(parse)
		if err != nil {
			return nil, err
		}
		return closerStorage{
			Storage: newStorage(fs),
			closer:  func() error { return nil },
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedScheme, parse.Scheme)
	}
}

func newStorage(fs afero.Fs) Storage {
	return &storageFS{Fs: fs}
}

func (s *storageFS) Close() error {
	if closer, ok := s.Fs.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (s *storageFS) Child(paths ...string) Storage {
	childPath := utils.MustChild(paths...)
	if childPath == "" {
		return s
	}

	return &storageFS{
		Fs: &scopedFS{base: s.Fs, prefix: childPath},
	}
}

func resolveLocalRoot(parse *url.URL) (string, error) {
	root := parse.Path
	if parse.Host != "" {
		root = path.Join("/", parse.Host, root)
	}
	if root == "" {
		return "", ErrInvalidPath
	}
	return root, nil
}

func newOverlayStorageFromURL(parse *url.URL) (CloserStorage, error) {
	query := parse.Query()

	rootURL := firstNonEmpty(query.Get("upperdir"), query.Get("root"))
	if rootURL == "" {
		return nil, fmt.Errorf("%w: missing upperdir", ErrInvalidMountSpec)
	}

	root, err := NewStorageFromURL(rootURL)
	if err != nil {
		return nil, err
	}

	mountFS := NewMountFS(root)
	closers := []io.Closer{root}

	for _, spec := range query["mount"] {
		prefix, targetURL, err := parseMountSpec(spec)
		if err != nil {
			_ = closeAll(closers)
			return nil, err
		}

		target, err := NewStorageFromURL(targetURL)
		if err != nil {
			_ = closeAll(closers)
			return nil, err
		}
		closers = append(closers, target)

		if err := mountFS.Mount(prefix, target); err != nil {
			_ = closeAll(closers)
			return nil, err
		}
	}

	return closerStorage{
		Storage: newStorage(mountFS),
		closer: func() error {
			return closeAll(closers)
		},
	}, nil
}

func parseMountSpec(spec string) (string, string, error) {
	prefix, rawURL, ok := strings.Cut(spec, "::")
	if !ok {
		return "", "", fmt.Errorf("%w: mount must use <prefix>::<storage-url>", ErrInvalidMountSpec)
	}
	prefix = NormalizeMountPath(prefix)
	if prefix == "/" {
		return "", "", fmt.Errorf("%w: mount prefix must not be /", ErrInvalidMountSpec)
	}
	if rawURL == "" {
		return "", "", fmt.Errorf("%w: empty mount target", ErrInvalidMountSpec)
	}
	return prefix, rawURL, nil
}

func closeAll(closers []io.Closer) error {
	slices.Reverse(closers)
	var errs []error
	for _, closer := range closers {
		if closer == nil {
			continue
		}
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

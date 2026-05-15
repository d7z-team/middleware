package storage

import (
	"context"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

type s3File struct {
	fs       *s3FS
	path     string
	key      string
	tmp      *os.File
	writable bool
	dirty    bool
	created  bool
	append   bool
	closed   bool
}

type s3DirFile struct {
	fs      *s3FS
	path    string
	entries []os.FileInfo
	offset  int
	closed  bool
}

type s3FileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	dir     bool
}

func (f s3FileInfo) Name() string       { return f.name }
func (f s3FileInfo) Size() int64        { return f.size }
func (f s3FileInfo) Mode() os.FileMode  { return f.mode }
func (f s3FileInfo) ModTime() time.Time { return f.modTime }
func (f s3FileInfo) IsDir() bool        { return f.dir }
func (f s3FileInfo) Sys() any           { return nil }

func (f *s3File) Close() error {
	if f.closed {
		return nil
	}
	if f.writable && (f.dirty || f.created) {
		if err := f.Sync(); err != nil {
			_ = f.tmp.Close()
			_ = os.Remove(f.tmp.Name())
			f.closed = true
			return err
		}
	}

	f.closed = true
	err := f.tmp.Close()
	removeErr := os.Remove(f.tmp.Name())
	if err != nil {
		return err
	}
	return removeErr
}

func (f *s3File) Read(p []byte) (int, error) {
	return f.tmp.Read(p)
}

func (f *s3File) ReadAt(p []byte, off int64) (int, error) {
	return f.tmp.ReadAt(p, off)
}

func (f *s3File) Seek(offset int64, whence int) (int64, error) {
	return f.tmp.Seek(offset, whence)
}

func (f *s3File) Write(p []byte) (int, error) {
	if f.append {
		if _, err := f.tmp.Seek(0, io.SeekEnd); err != nil {
			return 0, err
		}
	}
	n, err := f.tmp.Write(p)
	if n > 0 {
		f.dirty = true
	}
	return n, err
}

func (f *s3File) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.tmp.WriteAt(p, off)
	if n > 0 {
		f.dirty = true
	}
	return n, err
}

func (f *s3File) Name() string {
	return path.Base(f.path)
}

func (f *s3File) Readdir(count int) ([]os.FileInfo, error) {
	return nil, &os.PathError{Op: "readdir", Path: f.path, Err: iofs.ErrInvalid}
}

func (f *s3File) Readdirnames(count int) ([]string, error) {
	return nil, &os.PathError{Op: "readdir", Path: f.path, Err: iofs.ErrInvalid}
}

func (f *s3File) Stat() (os.FileInfo, error) {
	info, err := f.tmp.Stat()
	if err != nil {
		return nil, err
	}
	return s3FileInfo{
		name:    path.Base(f.path),
		size:    info.Size(),
		mode:    0o644,
		modTime: info.ModTime(),
	}, nil
}

func (f *s3File) Sync() error {
	if f.closed {
		return os.ErrClosed
	}
	if err := f.tmp.Sync(); err != nil {
		return err
	}
	if !f.writable || (!f.dirty && !f.created) {
		return nil
	}

	offset, err := f.tmp.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if _, err := f.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err = f.fs.transfer.UploadObject(context.Background(), &transfermanager.UploadObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.key),
		Body:   f.tmp,
	})
	if _, seekErr := f.tmp.Seek(offset, io.SeekStart); seekErr != nil && err == nil {
		err = seekErr
	}
	if err != nil {
		return err
	}
	f.dirty = false
	f.created = false
	return nil
}

func (f *s3File) Truncate(size int64) error {
	if err := f.tmp.Truncate(size); err != nil {
		return err
	}
	f.dirty = true
	return nil
}

func (f *s3File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

func (d *s3DirFile) Close() error {
	d.closed = true
	return nil
}

func (d *s3DirFile) Read([]byte) (int, error) {
	return 0, &os.PathError{Op: "read", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) ReadAt([]byte, int64) (int, error) {
	return 0, &os.PathError{Op: "read", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset == 0 {
		d.offset = 0
		return 0, nil
	}
	return 0, &os.PathError{Op: "seek", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) Write([]byte) (int, error) {
	return 0, &os.PathError{Op: "write", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) WriteAt([]byte, int64) (int, error) {
	return 0, &os.PathError{Op: "write", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) Name() string {
	if d.path == "" {
		return d.fs.bucket
	}
	return path.Base(d.path)
}

func (d *s3DirFile) Readdir(count int) ([]os.FileInfo, error) {
	if count <= 0 {
		if d.offset >= len(d.entries) {
			return nil, nil
		}
		rest := append([]os.FileInfo(nil), d.entries[d.offset:]...)
		d.offset = len(d.entries)
		return rest, nil
	}
	if d.offset >= len(d.entries) {
		return nil, io.EOF
	}

	end := d.offset + count
	if end > len(d.entries) {
		end = len(d.entries)
	}
	chunk := append([]os.FileInfo(nil), d.entries[d.offset:end]...)
	d.offset = end
	return chunk, nil
}

func (d *s3DirFile) Readdirnames(count int) ([]string, error) {
	infos, err := d.Readdir(count)
	if err != nil && err != io.EOF {
		return nil, err
	}
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name())
	}
	if err == io.EOF {
		return names, io.EOF
	}
	return names, nil
}

func (d *s3DirFile) Stat() (os.FileInfo, error) {
	return d.fs.Stat(d.path)
}

func (d *s3DirFile) Sync() error {
	return nil
}

func (d *s3DirFile) Truncate(int64) error {
	return &os.PathError{Op: "truncate", Path: d.path, Err: iofs.ErrInvalid}
}

func (d *s3DirFile) WriteString(string) (int, error) {
	return 0, &os.PathError{Op: "write", Path: d.path, Err: iofs.ErrInvalid}
}

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/afero"
	"gopkg.d7z.net/middleware/connects"
)

type s3FS struct {
	client   *s3sdk.Client
	transfer *transfermanager.Client
	bucket   string
	prefix   string
	tempDir  string
	name     string
}

func newS3StorageFromURL(parse *url.URL) (afero.Fs, error) {
	target, err := connects.NewS3(parse)
	if err != nil {
		if errors.Is(err, connects.ErrInvalidS3Config) {
			return nil, fmt.Errorf("%w: %s", ErrInvalidPath, err)
		}
		return nil, err
	}

	return &s3FS{
		client:   target.Client,
		transfer: transfermanager.New(target.Client),
		bucket:   target.Bucket,
		prefix:   target.Prefix,
		tempDir:  target.TempDir,
		name:     "S3FS",
	}, nil
}

func normalizeS3Path(name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	if name == "" || name == "." || name == "/" {
		return "", nil
	}

	parts := strings.Split(strings.Trim(name, "/"), "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "", ".", "..":
			return "", ErrInvalidPath
		default:
			cleaned = append(cleaned, part)
		}
	}
	return strings.Join(cleaned, "/"), nil
}

func (fs *s3FS) Create(name string) (afero.File, error) {
	return fs.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o666)
}

func (fs *s3FS) Mkdir(name string, _ os.FileMode) error {
	rel, err := normalizeS3Path(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}

	if _, _, err := fs.statPath(rel); err == nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := fs.ensureParentDir("mkdir", name, rel); err != nil {
		return err
	}
	return fs.putDirMarker(rel)
}

func (fs *s3FS) MkdirAll(name string, _ os.FileMode) error {
	rel, err := normalizeS3Path(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return nil
	}

	current := ""
	for _, part := range strings.Split(rel, "/") {
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}

		info, _, err := fs.statPath(current)
		if err == nil {
			if !info.IsDir() {
				return &os.PathError{Op: "mkdir", Path: name, Err: os.ErrExist}
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if err := fs.putDirMarker(current); err != nil {
			return err
		}
	}

	return nil
}

func (fs *s3FS) Open(name string) (afero.File, error) {
	info, rel, err := fs.statPath(name)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return fs.openDir(rel)
	}
	return fs.openObject(rel, os.O_RDONLY, 0o644)
}

func (fs *s3FS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	info, rel, statErr := fs.statPath(name)
	if statErr == nil && info.IsDir() {
		if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND) != 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: iofs.ErrInvalid}
		}
		return fs.openDir(rel)
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, statErr
	}

	rel, err := normalizeS3Path(name)
	if err != nil {
		return nil, err
	}
	if rel == "" {
		return nil, &os.PathError{Op: "open", Path: name, Err: iofs.ErrInvalid}
	}

	exists := statErr == nil
	if !exists && flag&os.O_CREATE == 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
	}
	if exists && flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	}

	if err := fs.ensureParentDir("open", name, rel); err != nil {
		return nil, err
	}

	return fs.openObject(rel, flag, perm)
}

func (fs *s3FS) Remove(name string) error {
	info, rel, err := fs.statPath(name)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		_, err := fs.client.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(fs.fileKey(rel)),
		})
		return err
	}

	entries, err := fs.readDir(rel)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return &os.PathError{Op: "remove", Path: name, Err: ErrDirectoryNotEmpty}
	}
	if rel == "" {
		return nil
	}

	_, err = fs.client.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.dirKey(rel)),
	})
	return err
}

func (fs *s3FS) RemoveAll(name string) error {
	rel, err := normalizeS3Path(name)
	if err != nil {
		return err
	}
	if rel == "" {
		return fs.deletePrefix(fs.dirKey(""))
	}

	_, err = fs.client.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.fileKey(rel)),
	})
	if err != nil && !isS3NotFound(err) {
		return err
	}
	return fs.deletePrefix(fs.dirKey(rel))
}

func (fs *s3FS) Rename(oldname, newname string) error {
	oldInfo, oldRel, err := fs.statPath(oldname)
	if err != nil {
		return err
	}

	newRel, err := normalizeS3Path(newname)
	if err != nil {
		return err
	}
	if newRel == "" {
		return &os.PathError{Op: "rename", Path: newname, Err: iofs.ErrInvalid}
	}

	if _, _, err := fs.statPath(newRel); err == nil {
		return &os.PathError{Op: "rename", Path: newname, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if err := fs.ensureParentDir("rename", newname, newRel); err != nil {
		return err
	}

	if !oldInfo.IsDir() {
		if err := fs.copyObject(fs.fileKey(oldRel), fs.fileKey(newRel)); err != nil {
			return err
		}
		_, err := fs.client.DeleteObject(context.Background(), &s3sdk.DeleteObjectInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(fs.fileKey(oldRel)),
		})
		return err
	}

	prefix := fs.dirKey(oldRel)
	var objects []string
	paginator := s3sdk.NewListObjectsV2Paginator(fs.client, &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key != "" {
				objects = append(objects, key)
			}
		}
	}

	copied := make([]string, 0, len(objects))
	for _, key := range objects {
		suffix := strings.TrimPrefix(key, prefix)
		destKey := fs.dirKey(newRel) + suffix
		if err := fs.copyObject(key, destKey); err != nil {
			_ = fs.deleteObjects(copied)
			return err
		}
		copied = append(copied, destKey)
	}

	if len(objects) == 0 {
		if err := fs.putDirMarker(newRel); err != nil {
			return err
		}
	}

	return fs.deleteObjects(objects)
}

func (fs *s3FS) Stat(name string) (os.FileInfo, error) {
	info, _, err := fs.statPath(name)
	return info, err
}

func (fs *s3FS) Name() string {
	return fs.name
}

func (fs *s3FS) Chmod(name string, _ os.FileMode) error {
	_, _, err := fs.statPath(name)
	return err
}

func (fs *s3FS) Chown(name string, _, _ int) error {
	_, _, err := fs.statPath(name)
	return err
}

func (fs *s3FS) Chtimes(name string, _, _ time.Time) error {
	_, _, err := fs.statPath(name)
	return err
}

func (fs *s3FS) statPath(name string) (os.FileInfo, string, error) {
	rel, err := normalizeS3Path(name)
	if err != nil {
		return nil, "", err
	}
	if rel == "" {
		rootName := fs.bucket
		if fs.prefix != "" {
			rootName = path.Base(fs.prefix)
		}
		return s3FileInfo{name: rootName, mode: os.ModeDir | 0o755, dir: true}, "", nil
	}

	fileHead, err := fs.client.HeadObject(context.Background(), &s3sdk.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.fileKey(rel)),
	})
	if err == nil {
		return s3FileInfo{
			name:    path.Base(rel),
			size:    aws.ToInt64(fileHead.ContentLength),
			mode:    0o644,
			modTime: aws.ToTime(fileHead.LastModified),
		}, rel, nil
	}
	if !isS3NotFound(err) {
		return nil, rel, err
	}

	dirHead, err := fs.client.HeadObject(context.Background(), &s3sdk.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.dirKey(rel)),
	})
	if err == nil {
		return s3FileInfo{
			name:    path.Base(rel),
			mode:    os.ModeDir | 0o755,
			modTime: aws.ToTime(dirHead.LastModified),
			dir:     true,
		}, rel, nil
	}
	if !isS3NotFound(err) {
		return nil, rel, err
	}

	page, err := fs.client.ListObjectsV2(context.Background(), &s3sdk.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(fs.dirKey(rel)),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return nil, rel, err
	}
	if len(page.Contents) != 0 || len(page.CommonPrefixes) != 0 {
		return s3FileInfo{name: path.Base(rel), mode: os.ModeDir | 0o755, dir: true}, rel, nil
	}
	return nil, rel, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
}

func (fs *s3FS) openObject(rel string, flag int, _ os.FileMode) (afero.File, error) {
	tmp, err := os.CreateTemp(fs.tempDir, "middleware-s3-*")
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}

	info, _, statErr := fs.statPath(rel)
	exists := statErr == nil && !info.IsDir()
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		cleanup()
		return nil, statErr
	}

	if exists && flag&os.O_TRUNC == 0 {
		if _, err := fs.transfer.DownloadObject(context.Background(), &transfermanager.DownloadObjectInput{
			Bucket:   aws.String(fs.bucket),
			Key:      aws.String(fs.fileKey(rel)),
			WriterAt: tmp,
		}); err != nil {
			cleanup()
			return nil, err
		}
	}

	if flag&os.O_TRUNC != 0 {
		if err := tmp.Truncate(0); err != nil {
			cleanup()
			return nil, err
		}
	}

	if flag&os.O_APPEND != 0 {
		_, err = tmp.Seek(0, io.SeekEnd)
	} else {
		_, err = tmp.Seek(0, io.SeekStart)
	}
	if err != nil {
		cleanup()
		return nil, err
	}

	return &s3File{
		fs:       fs,
		path:     rel,
		key:      fs.fileKey(rel),
		tmp:      tmp,
		writable: flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC) != 0,
		dirty:    flag&os.O_TRUNC != 0,
		created:  !exists && flag&os.O_CREATE != 0,
		append:   flag&os.O_APPEND != 0,
	}, nil
}

func (fs *s3FS) openDir(rel string) (afero.File, error) {
	entries, err := fs.readDir(rel)
	if err != nil {
		return nil, err
	}
	return &s3DirFile{fs: fs, path: rel, entries: entries}, nil
}

func (fs *s3FS) readDir(rel string) ([]os.FileInfo, error) {
	if rel != "" {
		info, _, err := fs.statPath(rel)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, &os.PathError{Op: "readdir", Path: rel, Err: iofs.ErrInvalid}
		}
	}

	prefix := fs.dirKey(rel)
	paginator := s3sdk.NewListObjectsV2Paginator(fs.client, &s3sdk.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	files := make(map[string]os.FileInfo)
	dirs := make(map[string]os.FileInfo)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key == prefix {
				continue
			}
			name := strings.TrimPrefix(key, prefix)
			if name == "" || strings.Contains(name, "/") {
				continue
			}
			files[name] = s3FileInfo{
				name:    name,
				size:    aws.ToInt64(object.Size),
				mode:    0o644,
				modTime: aws.ToTime(object.LastModified),
			}
		}

		for _, objectPrefix := range page.CommonPrefixes {
			name := strings.TrimPrefix(strings.TrimSuffix(aws.ToString(objectPrefix.Prefix), "/"), prefix)
			if name == "" || files[name] != nil {
				continue
			}
			dirs[name] = s3FileInfo{name: name, mode: os.ModeDir | 0o755, dir: true}
		}
	}

	entries := make([]os.FileInfo, 0, len(files)+len(dirs))
	for _, info := range files {
		entries = append(entries, info)
	}
	for _, info := range dirs {
		entries = append(entries, info)
	}
	slices.SortFunc(entries, func(a, b os.FileInfo) int {
		return strings.Compare(a.Name(), b.Name())
	})
	return entries, nil
}

func (fs *s3FS) deletePrefix(prefix string) error {
	paginator := s3sdk.NewListObjectsV2Paginator(fs.client, &s3sdk.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			if key != "" {
				keys = append(keys, key)
			}
		}
	}

	return fs.deleteObjects(keys)
}

func (fs *s3FS) deleteObjects(keys []string) error {
	for len(keys) > 0 {
		batch := keys
		if len(batch) > 1000 {
			batch = batch[:1000]
		}

		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}

		_, err := fs.client.DeleteObjects(context.Background(), &s3sdk.DeleteObjectsInput{
			Bucket: aws.String(fs.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}

		keys = keys[len(batch):]
	}
	return nil
}

func (fs *s3FS) copyObject(srcKey, dstKey string) error {
	_, err := fs.client.CopyObject(context.Background(), &s3sdk.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		CopySource: aws.String(url.PathEscape(fs.bucket) + "/" + url.PathEscape(srcKey)),
		Key:        aws.String(dstKey),
	})
	return err
}

func (fs *s3FS) putDirMarker(rel string) error {
	_, err := fs.client.PutObject(context.Background(), &s3sdk.PutObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(fs.dirKey(rel)),
		Body:   strings.NewReader(""),
	})
	return err
}

func (fs *s3FS) ensureParentDir(op, name, rel string) error {
	parent := path.Dir(rel)
	if parent == "." {
		return nil
	}

	info, _, err := fs.statPath(parent)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return nil
	}
	return &os.PathError{Op: op, Path: name, Err: iofs.ErrInvalid}
}

func (fs *s3FS) fileKey(rel string) string {
	if rel == "" {
		return fs.prefix
	}
	if fs.prefix == "" {
		return rel
	}
	return fs.prefix + "/" + rel
}

func (fs *s3FS) dirKey(rel string) string {
	if rel == "" {
		if fs.prefix == "" {
			return ""
		}
		return fs.prefix + "/"
	}
	return fs.fileKey(rel) + "/"
}

func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}

	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "NoSuchBucket":
			return true
		}
	}
	return false
}

package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type testS3Target struct {
	endpoint  string
	region    string
	bucket    string
	accessKey string
	secretKey string
}

type testS3TargetState struct {
	target testS3Target
	skip   string
	err    error
}

var (
	testS3TargetOnce       sync.Once
	testS3TargetStateValue testS3TargetState
)

func TestS3SpecificBehaviors(t *testing.T) {
	target := loadTestS3Target(t)

	t.Run("empty_directory_markers_and_parent_dirs", func(t *testing.T) {
		fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "dirs"))
		defer fs.Close()

		require.NoError(t, fs.MkdirAll("a/b/c", 0o755))

		for _, name := range []string{"a", "a/b", "a/b/c"} {
			info, err := fs.Stat(name)
			require.NoError(t, err)
			require.True(t, info.IsDir())
		}
	})

	t.Run("remove_non_empty_directory", func(t *testing.T) {
		fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "remove"))
		defer fs.Close()

		require.NoError(t, fs.MkdirAll("root/sub", 0o755))
		require.NoError(t, afero.WriteFile(fs, "root/sub/file.txt", []byte("x"), 0o644))

		err := fs.Remove("root")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDirectoryNotEmpty)
	})

	t.Run("rename_directory_tree", func(t *testing.T) {
		fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "rename"))
		defer fs.Close()

		require.NoError(t, fs.MkdirAll("docs/nested", 0o755))
		require.NoError(t, fs.MkdirAll("archive", 0o755))
		require.NoError(t, afero.WriteFile(fs, "docs/nested/file.txt", []byte("payload"), 0o644))

		require.NoError(t, fs.Rename("docs", "archive/docs"))

		_, err := fs.Stat("docs")
		require.Error(t, err)

		data, err := afero.ReadFile(fs, "archive/docs/nested/file.txt")
		require.NoError(t, err)
		require.Equal(t, "payload", string(data))
	})

	t.Run("sync_and_reopen", func(t *testing.T) {
		fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "sync"))
		defer fs.Close()

		file, err := fs.OpenFile("sync.txt", os.O_CREATE|os.O_RDWR, 0o644)
		require.NoError(t, err)
		_, err = file.Write([]byte("draft"))
		require.NoError(t, err)
		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())

		data, err := afero.ReadFile(fs, "sync.txt")
		require.NoError(t, err)
		require.Equal(t, "draft", string(data))
	})

	t.Run("metadata_ops_are_existence_checked_noops", func(t *testing.T) {
		fs := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "meta"))
		defer fs.Close()

		require.NoError(t, afero.WriteFile(fs, "meta.txt", []byte("x"), 0o644))
		require.NoError(t, fs.Chmod("meta.txt", 0o600))
		require.NoError(t, fs.Chown("meta.txt", 1000, 1000))
		require.NoError(t, fs.Chtimes("meta.txt", time.Now(), time.Now()))

		require.ErrorIs(t, fs.Chmod("missing.txt", 0o600), os.ErrNotExist)
		require.ErrorIs(t, fs.Chown("missing.txt", 1000, 1000), os.ErrNotExist)
		require.ErrorIs(t, fs.Chtimes("missing.txt", time.Now(), time.Now()), os.ErrNotExist)
	})

	t.Run("root_prefix_isolation", func(t *testing.T) {
		left := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "left"))
		right := newTestS3StorageAtPrefix(t, target, testS3Prefix(t, "right"))
		defer left.Close()
		defer right.Close()

		require.NoError(t, afero.WriteFile(left, "shared.txt", []byte("left"), 0o644))
		require.NoError(t, afero.WriteFile(right, "shared.txt", []byte("right"), 0o644))

		leftData, err := afero.ReadFile(left, "shared.txt")
		require.NoError(t, err)
		rightData, err := afero.ReadFile(right, "shared.txt")
		require.NoError(t, err)

		require.Equal(t, "left", string(leftData))
		require.Equal(t, "right", string(rightData))
	})
}

func newTestS3StorageAtPrefix(t *testing.T, target testS3Target, prefix string) CloserStorage {
	t.Helper()

	rawURL := url.URL{
		Scheme: "s3",
		Host:   target.bucket,
		Path:   "/" + prefix,
		RawQuery: url.Values{
			"region":      []string{target.region},
			"endpoint":    []string{target.endpoint},
			"path_style":  []string{"true"},
			"access_key":  []string{target.accessKey},
			"secret_key":  []string{target.secretKey},
			"disable_ssl": []string{"true"},
		}.Encode(),
	}

	store, err := NewStorageFromURL(rawURL.String())
	require.NoError(t, err)

	return closerStorage{
		Storage: store,
		closer: func() error {
			cleanupErr := store.RemoveAll("")
			closeErr := store.Close()
			return errors.Join(cleanupErr, closeErr)
		},
	}
}

func loadTestS3Target(t *testing.T) testS3Target {
	t.Helper()

	testS3TargetOnce.Do(func() {
		testS3TargetStateValue = prepareTestS3Target()
	})
	if testS3TargetStateValue.skip != "" {
		t.Skipf("minio unavailable: %v", testS3TargetStateValue.skip)
	}
	require.NoError(t, testS3TargetStateValue.err)
	return testS3TargetStateValue.target
}

func prepareTestS3Target() testS3TargetState {
	target := testS3Target{
		endpoint:  firstNonEmpty(os.Getenv("MIDDLEWARE_TEST_S3_ENDPOINT"), "http://127.0.0.1:9000"),
		region:    firstNonEmpty(os.Getenv("MIDDLEWARE_TEST_S3_REGION"), "us-east-1"),
		bucket:    firstNonEmpty(os.Getenv("MIDDLEWARE_TEST_S3_BUCKET"), "middleware-storage-test"),
		accessKey: firstNonEmpty(os.Getenv("MIDDLEWARE_TEST_S3_ACCESS_KEY"), "minioadmin"),
		secretKey: firstNonEmpty(os.Getenv("MIDDLEWARE_TEST_S3_SECRET_KEY"), "minioadmin"),
	}

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(target.region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(target.accessKey, target.secretKey, "")),
		config.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return testS3TargetState{target: target, err: err}
	}

	client := s3sdk.NewFromConfig(cfg, func(options *s3sdk.Options) {
		options.BaseEndpoint = aws.String(target.endpoint)
		options.UsePathStyle = true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err = client.ListBuckets(ctx, &s3sdk.ListBucketsInput{})
	cancel()
	if err != nil {
		return testS3TargetState{target: target, skip: err.Error()}
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	_, err = client.HeadBucket(ctx, &s3sdk.HeadBucketInput{Bucket: aws.String(target.bucket)})
	cancel()
	if err == nil {
		return testS3TargetState{target: target}
	}
	if !isS3NotFound(err) {
		return testS3TargetState{target: target, skip: err.Error()}
	}

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	_, err = client.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String(target.bucket)})
	cancel()
	if err != nil {
		return testS3TargetState{target: target, err: err}
	}
	return testS3TargetState{target: target}
}

func testS3Prefix(t *testing.T, suffix string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", ":", "-").Replace(t.Name())
	return fmt.Sprintf("%s-%s-%d", name, suffix, time.Now().UnixNano())
}

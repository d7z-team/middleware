package connects

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

var ErrInvalidS3Config = errors.New("invalid s3 configuration")

const defaultS3HTTPTimeout = 5 * time.Minute

type S3Target struct {
	Client  *s3sdk.Client
	Bucket  string
	Prefix  string
	TempDir string
}

func NewS3(ur *url.URL) (*S3Target, error) {
	if ur.Host == "" {
		return nil, fmt.Errorf("%w: bucket is required", ErrInvalidS3Config)
	}

	prefix, err := normalizeS3URLPath(ur.Path)
	if err != nil {
		return nil, err
	}

	query := ur.Query()
	pathStyle, err := parseS3QueryBool(query, "path_style")
	if err != nil {
		return nil, err
	}
	disableSSL, err := parseS3QueryBool(query, "disable_ssl")
	if err != nil {
		return nil, err
	}
	httpTimeout, err := parseS3HTTPTimeout(query)
	if err != nil {
		return nil, err
	}

	tempDir := query.Get("temp_dir")
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: create temp_dir: %w", ErrInvalidS3Config, err)
	}

	region := strings.TrimSpace(query.Get("region"))
	endpoint := strings.TrimSpace(query.Get("endpoint"))
	if endpoint != "" && !strings.Contains(endpoint, "://") {
		scheme := "https://"
		if disableSSL {
			scheme = "http://"
		}
		endpoint = scheme + endpoint
	}
	if endpoint != "" && region == "" {
		region = "us-east-1"
	}

	loadOptions := make([]func(*config.LoadOptions) error, 0, 3)
	if region != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}
	if profile := strings.TrimSpace(query.Get("profile")); profile != "" {
		loadOptions = append(loadOptions, config.WithSharedConfigProfile(profile))
	}
	if httpTimeout > 0 {
		loadOptions = append(loadOptions, config.WithHTTPClient(
			awshttp.NewBuildableClient().WithTimeout(httpTimeout),
		))
	}

	accessKey := strings.TrimSpace(query.Get("access_key"))
	secretKey := strings.TrimSpace(query.Get("secret_key"))
	sessionToken := strings.TrimSpace(query.Get("session_token"))
	if accessKey != "" || secretKey != "" || sessionToken != "" {
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("%w: access_key and secret_key must both be set", ErrInvalidS3Config)
		}
		loadOptions = append(loadOptions, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, err
	}

	client := s3sdk.NewFromConfig(cfg, func(options *s3sdk.Options) {
		options.UsePathStyle = pathStyle
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
	})

	return &S3Target{
		Client:  client,
		Bucket:  ur.Host,
		Prefix:  prefix,
		TempDir: tempDir,
	}, nil
}

func parseS3QueryBool(query url.Values, key string) (bool, error) {
	value := strings.TrimSpace(query.Get(key))
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%w: invalid %s", ErrInvalidS3Config, key)
	}
	return parsed, nil
}

func parseS3HTTPTimeout(query url.Values) (time.Duration, error) {
	value := strings.TrimSpace(query.Get("http_timeout"))
	if value == "" {
		return defaultS3HTTPTimeout, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%w: invalid http_timeout", ErrInvalidS3Config)
	}
	return parsed, nil
}

func normalizeS3URLPath(raw string) (string, error) {
	raw = strings.ReplaceAll(raw, "\\", "/")
	if raw == "" || raw == "." || raw == "/" {
		return "", nil
	}

	parts := strings.Split(strings.Trim(raw, "/"), "/")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "", ".", "..":
			return "", fmt.Errorf("%w: invalid path", ErrInvalidS3Config)
		default:
			cleaned = append(cleaned, part)
		}
	}
	return strings.Join(cleaned, "/"), nil
}

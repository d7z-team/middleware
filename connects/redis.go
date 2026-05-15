package connects

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisOption defines the configuration options for Redis.
type RedisOption func(*redis.Options)

// WithRedisAuth sets the authentication credentials.
func WithRedisAuth(username, password string) RedisOption {
	return func(o *redis.Options) {
		o.Username = username
		o.Password = password
	}
}

// WithRedisDB sets the database number.
func WithRedisDB(db int) RedisOption {
	return func(o *redis.Options) {
		o.DB = db
	}
}

// WithRedisTLS enables TLS configuration.
func WithRedisTLS(config *tls.Config) RedisOption {
	return func(o *redis.Options) {
		o.TLSConfig = config
	}
}

// WithRedisPool configures the connection pool.
func WithRedisPool(size, minIdle int) RedisOption {
	return func(o *redis.Options) {
		o.PoolSize = size
		o.MinIdleConns = minIdle
	}
}

// WithRedisTimeouts configures the timeouts.
func WithRedisTimeouts(dial, read, write time.Duration) RedisOption {
	return func(o *redis.Options) {
		if dial > 0 {
			o.DialTimeout = dial
		}
		if read > 0 {
			o.ReadTimeout = read
		}
		if write > 0 {
			o.WriteTimeout = write
		}
	}
}

// ConnectRedis connects to Redis with the given address and options.
func ConnectRedis(addr string, opts ...RedisOption) (*redis.Client, error) {
	if addr == "" {
		addr = "localhost:6379"
	}

	if !strings.Contains(addr, ":") {
		addr = net.JoinHostPort(addr, "6379")
	} else if _, _, err := net.SplitHostPort(addr); err != nil {
		if strings.Contains(err.Error(), "missing port") {
			addr = net.JoinHostPort(addr, "6379")
		}
	}

	options := &redis.Options{
		Addr: addr,
	}

	for _, opt := range opts {
		opt(options)
	}

	client := redis.NewClient(options)
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return client, nil
}

// NewRedis creates a Redis client from a URL configuration.
// URL format: redis://[user:password@]host:port/db
// Supported parameters:
// - insecure=true: Skip TLS verification (rediss scheme)
// - pool_size, min_idle: Connection pool settings
// - dial_timeout, read_timeout, write_timeout: Timeout settings
func NewRedis(ur *url.URL) (*redis.Client, error) {
	addr := ur.Host
	query := ur.Query()
	var parsedOpts []RedisOption

	// Auth
	if ur.User != nil {
		username := ur.User.Username()
		password, _ := ur.User.Password()
		parsedOpts = append(parsedOpts, WithRedisAuth(username, password))
	}

	// DB
	if ur.Path != "" && ur.Path != "/" {
		dbStr := strings.TrimPrefix(ur.Path, "/")
		dbInt, err := strconv.Atoi(dbStr)
		if err != nil || dbInt < 0 {
			return nil, errors.New("Redis DB number must be an integer: " + dbStr)
		}
		parsedOpts = append(parsedOpts, WithRedisDB(dbInt))
	}

	// TLS
	tlsKeys := []string{"insecure", "ca-file", "cert-file", "key-file", "server_name"}
	if !strings.EqualFold(ur.Scheme, "rediss") {
		for _, key := range tlsKeys {
			if query.Get(key) != "" {
				return nil, errors.New("tls parameters require rediss scheme")
			}
		}
	} else {
		if query.Get("insecure") == "true" {
			return nil, errors.New("redis insecure TLS via URL is not supported")
		}
		tlsConfig, err := loadTLSConfigFromFiles(
			query.Get("ca-file"),
			query.Get("cert-file"),
			query.Get("key-file"),
			query.Get("server_name"),
		)
		if err != nil {
			return nil, err
		}
		if tlsConfig == nil {
			tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		parsedOpts = append(parsedOpts, WithRedisTLS(tlsConfig))
	}

	// Additional options from query
	if sizeStr := query.Get("pool_size"); sizeStr != "" {
		size, err := strconv.Atoi(sizeStr)
		if err != nil || size <= 0 {
			return nil, errors.New("invalid redis pool_size: " + sizeStr)
		}
		parsedOpts = append(parsedOpts, func(o *redis.Options) { o.PoolSize = size })
	}
	if idleStr := query.Get("min_idle"); idleStr != "" {
		idle, err := strconv.Atoi(idleStr)
		if err != nil || idle < 0 {
			return nil, errors.New("invalid redis min_idle: " + idleStr)
		}
		parsedOpts = append(parsedOpts, func(o *redis.Options) { o.MinIdleConns = idle })
	}

	// Timeouts from query
	parseDuration := func(key string) (time.Duration, error) {
		if val := query.Get(key); val != "" {
			d, err := time.ParseDuration(val)
			if err != nil || d <= 0 {
				return 0, errors.New("invalid redis " + key + ": " + val)
			}
			return d, nil
		}
		return 0, nil
	}

	dialTimeout, err := parseDuration("dial_timeout")
	if err != nil {
		return nil, err
	}
	readTimeout, err := parseDuration("read_timeout")
	if err != nil {
		return nil, err
	}
	writeTimeout, err := parseDuration("write_timeout")
	if err != nil {
		return nil, err
	}
	if dialTimeout > 0 || readTimeout > 0 || writeTimeout > 0 {
		parsedOpts = append(parsedOpts, WithRedisTimeouts(dialTimeout, readTimeout, writeTimeout))
	}

	return ConnectRedis(addr, parsedOpts...)
}

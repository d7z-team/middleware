package connects

import (
	"context"
	"crypto/tls"
	"errors"
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
		return nil, errors.New("Redis connection failed: " + err.Error())
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
		if err != nil {
			return nil, errors.New("Redis DB number must be an integer: " + dbStr)
		}
		parsedOpts = append(parsedOpts, WithRedisDB(dbInt))
	}

	// TLS
	if strings.EqualFold(ur.Scheme, "rediss") {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: ur.Query().Get("insecure") == "true",
		}
		parsedOpts = append(parsedOpts, WithRedisTLS(tlsConfig))
	}

	// Additional options from query
	query := ur.Query()
	if sizeStr := query.Get("pool_size"); sizeStr != "" {
		if size, err := strconv.Atoi(sizeStr); err == nil {
			parsedOpts = append(parsedOpts, func(o *redis.Options) { o.PoolSize = size })
		}
	}
	if idleStr := query.Get("min_idle"); idleStr != "" {
		if idle, err := strconv.Atoi(idleStr); err == nil {
			parsedOpts = append(parsedOpts, func(o *redis.Options) { o.MinIdleConns = idle })
		}
	}

	// Timeouts from query
	parseDuration := func(key string) time.Duration {
		if val := query.Get(key); val != "" {
			d, _ := time.ParseDuration(val)
			return d
		}
		return 0
	}

	dialTimeout := parseDuration("dial_timeout")
	readTimeout := parseDuration("read_timeout")
	writeTimeout := parseDuration("write_timeout")
	if dialTimeout > 0 || readTimeout > 0 || writeTimeout > 0 {
		parsedOpts = append(parsedOpts, WithRedisTimeouts(dialTimeout, readTimeout, writeTimeout))
	}

	return ConnectRedis(addr, parsedOpts...)
}

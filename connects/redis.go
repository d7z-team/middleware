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

// RedisOption 定义 Redis 配置选项
// RedisOption defines the configuration options for Redis.
type RedisOption func(*redis.Options)

// WithRedisAuth 设置认证信息
// WithRedisAuth sets the authentication credentials.
func WithRedisAuth(username, password string) RedisOption {
	return func(o *redis.Options) {
		o.Username = username
		o.Password = password
	}
}

// WithRedisDB 设置数据库编号
// WithRedisDB sets the database number.
func WithRedisDB(db int) RedisOption {
	return func(o *redis.Options) {
		o.DB = db
	}
}

// WithRedisTLS 启用 TLS 配置
// WithRedisTLS enables TLS configuration.
func WithRedisTLS(config *tls.Config) RedisOption {
	return func(o *redis.Options) {
		o.TLSConfig = config
	}
}

// WithRedisPool 配置连接池
// WithRedisPool configures the connection pool.
func WithRedisPool(size, minIdle int) RedisOption {
	return func(o *redis.Options) {
		o.PoolSize = size
		o.MinIdleConns = minIdle
	}
}

// WithRedisTimeouts 配置超时时间
// WithRedisTimeouts configures the timeouts.
func WithRedisTimeouts(dial, read, write time.Duration) RedisOption {
	return func(o *redis.Options) {
		o.DialTimeout = dial
		o.ReadTimeout = read
		o.WriteTimeout = write
	}
}

// ConnectRedis 连接 Redis
// ConnectRedis connects to Redis with the given address and options.
func ConnectRedis(addr string, opts ...RedisOption) (*redis.Client, error) {
	if addr == "" {
		addr = "localhost:6379"
	}
	// Simple check for port, robust enough for typical hostname:port or ip:port
	// For IPv6 [::1], it has colon. For localhost, no colon -> add port.
	if !strings.Contains(addr, ":") {
		addr += ":6379"
	} else {
		// handle ipv6 without port case if necessary, or just rely on user providing valid addr.
		// net.SplitHostPort is safer.
		if _, _, err := net.SplitHostPort(addr); err != nil {
			// likely missing port or bad format.
			// If it's an IPv6 literal like "[::1]", SplitHostPort fails if no port?
			// net.JoinHostPort implementation checks for colons.
			// Let's assume if it fails, it might be just host.
			if strings.Count(addr, ":") > 1 && !strings.Contains(addr, "]") {
				// IPv6 literal without brackets? Invalid usually.
				// Assume it's host only if it doesn't look like an address with port.
			}
			// Fallback: append port if error indicates missing port
			if strings.Contains(err.Error(), "missing port") {
				addr = net.JoinHostPort(addr, "6379")
			}
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

// NewRedis 从 URL 解析配置创建 Redis 客户端
// NewRedis creates a Redis client from a URL configuration.
// URL format: redis://[user:password@]host:port/db?prefix=xxx
// Supported parameters:
// - user: Username (optional, Redis 6+)
// - password: Password (optional, :password format in URL)
// - host: Host (default localhost)
// - port: Port (default 6379)
// - db: Database number (default 0, specified in path /0)
// - query param 'insecure=true' for insecure skip verify (rediss scheme)
// - query param 'pool_size', 'min_idle', 'dial_timeout', 'read_timeout', 'write_timeout'
func NewRedis(ur *url.URL) (*redis.Client, error) {
	addr := ur.Host

	var parsedOpts []RedisOption

	// Auth
	var user, password string
	if ur.User != nil {
		user = ur.User.Username()
		password, _ = ur.User.Password()
	}
	parsedOpts = append(parsedOpts, WithRedisAuth(user, password))

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

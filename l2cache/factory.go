package l2cache

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

// NewCacheFromURL 通过 URL 配置创建缓存实例
// 支持的协议方案：
// - memory/mem: 内存缓存（带LRU），示例：memory://?max_capacity=1000&cleanup_interval=5m
// - redis: Redis 缓存（非TLS），示例：redis://:password@localhost:6379/0?prefix=app:cache:
// - rediss: Redis 缓存（TLS），示例：rediss://user:password@host:6380/1?prefix=cache:
func NewCacheFromURL(u string) (Cache, error) {
	// 解析 URL
	ur, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	// 根据协议方案创建对应缓存实例
	switch strings.ToLower(ur.Scheme) {
	case "memory", "mem":
		// 内存缓存：解析查询参数配置
		return newMemoryCacheFromURL(ur)
	case "redis", "rediss":
		// Redis 缓存：解析 URL 中的地址、密码、DB、前缀等
		return newRedisCacheFromURL(ur)
	default:
		return nil, errors.New("不支持的缓存协议: " + ur.Scheme)
	}
}

// newMemoryCacheFromURL 从 URL 解析配置创建内存缓存
// URL 格式：memory://?max_capacity=1000&cleanup_interval=5m
// 支持参数：
// - max_capacity: 最大缓存容量（必填，正整数）
// - cleanup_interval: 过期清理间隔（可选，默认5m，支持s/m/h单位）
func newMemoryCacheFromURL(ur *url.URL) (*MemoryCache, error) {
	query := ur.Query()

	// 1. 解析最大容量（必填）
	maxCap := 10 * 1024 * 1024
	var err error
	maxCapStr := query.Get("max_capacity")
	if maxCapStr != "" {
		maxCap, err = strconv.Atoi(maxCapStr)
		if err != nil || maxCap <= 0 {
			return nil, errors.New("max_capacity 必须是正整数")
		}
	}

	// 2. 解析清理间隔（可选，默认5m）
	cleanupIntervalStr := query.Get("cleanup_interval")
	cleanupInterval := 5 * time.Minute
	if cleanupIntervalStr != "" {
		dur, err := time.ParseDuration(cleanupIntervalStr)
		if err != nil {
			return nil, errors.New("cleanup_interval 格式无效（支持s/m/h，如5m）: " + err.Error())
		}
		cleanupInterval = dur
	}

	// 3. 创建内存缓存实例
	return NewMemoryCache(
		WithMaxCapacity(maxCap),
		WithCleanupInterval(cleanupInterval),
	)
}

// newRedisCacheFromURL 从 URL 解析配置创建 Redis 缓存
// URL 格式：redis://[user:password@]host:port/db?prefix=xxx
// 支持参数：
// - user: 用户名（可选，Redis 6+支持）
// - password: 密码（可选，URL中需用 :password 格式，如 redis://:123456@host）
// - host: 主机（默认localhost）
// - port: 端口（默认6379）
// - db: 数据库编号（默认0，URL路径中指定，如 /0）
// - prefix: 缓存键前缀（可选，默认"cache:"）
// - rediss 协议自动启用 TLS
func newRedisCacheFromURL(ur *url.URL) (*RedisCache, error) {
	// 1. 解析地址（host:port）
	addr := ur.Host
	if addr == "" {
		addr = "localhost:6379"
	} else if !strings.Contains(addr, ":") {
		// 只有主机，添加默认端口
		addr += ":6379"
	}

	// 2. 解析密码（URL User 部分，格式为 :password 或 user:password）
	var password string
	if ur.User != nil {
		password = ur.User.String()
		// 如果是 ":password" 格式，去掉前缀的冒号
		if strings.HasPrefix(password, ":") {
			password = password[1:]
		}
	}

	// 3. 解析数据库编号（URL 路径，如 /0 -> db=0）
	db := 0
	if ur.Path != "" && ur.Path != "/" {
		dbStr := strings.TrimPrefix(ur.Path, "/")
		dbInt, err := strconv.Atoi(dbStr)
		if err != nil {
			return nil, errors.New("Redis DB 编号必须是整数: " + dbStr)
		}
		db = dbInt
	}

	// 4. 解析键前缀（查询参数 prefix）
	query := ur.Query()
	prefix := query.Get("prefix")
	if prefix == "" {
		prefix = "cache:" // 默认前缀
	}

	// 5. 构建 Redis 客户端配置
	redisOpts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	// 6. rediss 协议启用 TLS
	if strings.ToLower(ur.Scheme) == "rediss" {
		redisOpts.TLSConfig = &tls.Config{}
		redisOpts.TLSConfig.InsecureSkipVerify = ur.Query().Get("insecure") == "true"
	}

	// 7. 创建 Redis 客户端和缓存实例
	client := redis.NewClient(redisOpts)
	// 验证连接（可选，提前检测连接问题）
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, errors.New("Redis 连接失败: " + err.Error())
	}

	return NewRedisCache(client, WithRedisPrefix(prefix)), nil
}

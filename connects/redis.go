package connects

import (
	"context"
	"crypto/tls"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

// NewRedis 从 URL 解析配置创建 Redis 缓存
// URL 格式：redis://[user:password@]host:port/db?prefix=xxx
// 支持参数：
// - user: 用户名（可选，Redis 6+支持）
// - password: 密码（可选，URL中需用 :password 格式，如 redis://:123456@host）
// - host: 主机（默认localhost）
// - port: 端口（默认6379）
// - db: 数据库编号（默认0，URL路径中指定，如 /0）
// - rediss 协议自动启用 TLS
func NewRedis(ur *url.URL) (*redis.Client, error) {
	addr := ur.Host
	if addr == "" {
		addr = "localhost:6379"
	} else if !strings.Contains(addr, ":") {
		// 只有主机，添加默认端口
		addr += ":6379"
	}

	var user, password string
	if ur.User != nil {
		user = ur.User.Username()
		password, _ = ur.User.Password()
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
	// 5. 构建 Redis 客户端配置
	redisOpts := &redis.Options{
		Addr:     addr,
		Username: user,
		Password: password,
		DB:       db,
	}

	if strings.EqualFold(ur.Scheme, "rediss") {
		redisOpts.TLSConfig = &tls.Config{}
		redisOpts.TLSConfig.InsecureSkipVerify = ur.Query().Get("insecure") == "true"
	}

	client := redis.NewClient(redisOpts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, errors.New("Redis 连接失败: " + err.Error())
	}

	return client, nil
}

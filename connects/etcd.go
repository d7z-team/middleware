package connects

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewEtcd(parse *url.URL) (*clientv3.Client, error) {
	query := parse.Query()
	endpoints := []string{parse.Host}

	// 检查是否有多个端点
	if endpointsStr := query.Get("endpoints"); endpointsStr != "" {
		endpoints = strings.Split(endpointsStr, ",")
	}

	// 检查是否需要TLS认证
	var tlsConfig *tls.Config
	caFile := query.Get("ca-file")
	certFile := query.Get("cert-file")
	keyFile := query.Get("key-file")

	if caFile != "" && certFile != "" && keyFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read ca file: %v", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.New("failed to add ca certificate")
		}

		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
	} else if caFile != "" || certFile != "" || keyFile != "" {
		// 部分TLS参数被提供，视为错误
		return nil, errors.New("incomplete tls configuration, need ca-file, cert-file and key-file")
	}
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints specified")
	}
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}
	return clientv3.New(cfg)
}

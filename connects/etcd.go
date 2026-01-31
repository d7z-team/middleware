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

// EtcdOption defines the configuration options for Etcd.
type EtcdOption func(*clientv3.Config)

// WithEtcdAuth sets the authentication credentials.
func WithEtcdAuth(username, password string) EtcdOption {
	return func(c *clientv3.Config) {
		c.Username = username
		c.Password = password
	}
}

// WithEtcdDialTimeout sets the dial timeout.
func WithEtcdDialTimeout(d time.Duration) EtcdOption {
	return func(c *clientv3.Config) {
		c.DialTimeout = d
	}
}

// WithEtcdTLS sets the TLS configuration.
func WithEtcdTLS(config *tls.Config) EtcdOption {
	return func(c *clientv3.Config) {
		c.TLS = config
	}
}

// ConnectEtcd connects to Etcd with the given endpoints and options.
func ConnectEtcd(endpoints []string, opts ...EtcdOption) (*clientv3.Client, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints specified")
	}

	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second, // Default timeout
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return clientv3.New(cfg)
}

// NewEtcd creates an Etcd client from a URL configuration.
// URL format: etcd://host:port?endpoints=host1:port,host2:port&ca-file=...
// Supported parameters:
// - endpoints: Comma-separated list of endpoints (overrides host in URL)
// - ca-file: Path to CA certificate file
// - cert-file: Path to client certificate file
// - key-file: Path to client key file
// - dial_timeout: Connection timeout (e.g., 5s)
func NewEtcd(u *url.URL) (*clientv3.Client, error) {
	query := u.Query()
	var endpoints []string

	if endpointsStr := query.Get("endpoints"); endpointsStr != "" {
		endpoints = strings.Split(endpointsStr, ",")
	} else if u.Host != "" {
		endpoints = []string{u.Host}
	}

	if len(endpoints) == 0 {
		return nil, errors.New("no etcd endpoints found in URL")
	}

	var parsedOpts []EtcdOption

	// TLS Configuration
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

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		parsedOpts = append(parsedOpts, WithEtcdTLS(tlsConfig))
	} else if caFile != "" || certFile != "" || keyFile != "" {
		return nil, errors.New("incomplete tls configuration, need ca-file, cert-file and key-file")
	}

	// Auth (from URL userinfo)
	if u.User != nil {
		username := u.User.Username()
		password, _ := u.User.Password()
		parsedOpts = append(parsedOpts, WithEtcdAuth(username, password))
	}

	// Dial Timeout
	if dtStr := query.Get("dial_timeout"); dtStr != "" {
		if dt, err := time.ParseDuration(dtStr); err == nil {
			parsedOpts = append(parsedOpts, WithEtcdDialTimeout(dt))
		}
	}

	return ConnectEtcd(endpoints, parsedOpts...)
}

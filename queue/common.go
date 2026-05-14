package queue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

type MessageID = string

// Producer publishes messages into a queue namespace.
type Producer interface {
	Publish(ctx context.Context, topic, body string, opts *PublishOptions) (MessageID, error)
	PublishBatch(ctx context.Context, topic string, messages []PublishRequest) ([]MessageID, error)
}

// Consumer claims visible messages from a queue namespace.
type Consumer interface {
	Consume(ctx context.Context, topic string, opts *ConsumeOptions) (*ClaimedMessage, error)
}

// Admin exposes queue inspection and management operations.
type Admin interface {
	Get(ctx context.Context, topic string, messageID MessageID) (*MessageState, error)
	Peek(ctx context.Context, topic string, limit int) ([]MessageState, error)
	Count(ctx context.Context, topic string) (*Stats, error)

	GetDead(ctx context.Context, topic string, messageID MessageID) (*DeadMessage, error)
	ListDead(ctx context.Context, topic string, opts *ListOptions) (*DeadListResult, error)
	CountDead(ctx context.Context, topic string) (int64, error)
	RequeueDead(ctx context.Context, topic string, messageID MessageID, delay time.Duration) error
	RequeueAllDead(ctx context.Context, topic string, limit int, delay time.Duration) (int, error)
	DeleteDead(ctx context.Context, topic string, messageID MessageID) error

	Cancel(ctx context.Context, topic string, messageID MessageID) error
	Delete(ctx context.Context, topic string, messageID MessageID) error
}

// Namespace provides scoped producer, consumer, and admin views.
type Namespace interface {
	Child(paths ...string) Namespace
	Producer() Producer
	Consumer() Consumer
	Admin() Admin
}

// CloserNamespace owns resources that can be closed.
type CloserNamespace interface {
	Namespace
	io.Closer
}

// PublishOptions controls optional publish-time queue behavior.
type PublishOptions struct {
	Delay         time.Duration
	DedupKey      string
	MaxDeliveries int
}

type PublishRequest struct {
	Body string
	Opts *PublishOptions
}

type ConsumeOptions struct {
	AckTimeout time.Duration
}

type ListOptions struct {
	Limit  int
	Cursor string
}

type Config struct {
	DefaultAckTimeout time.Duration
	RequeueInterval   time.Duration
	MaxDeliveries     int
}

type MessageStatus string

const (
	StatusReady    MessageStatus = "ready"
	StatusInflight MessageStatus = "inflight"
	StatusDead     MessageStatus = "dead"
	StatusDone     MessageStatus = "done"
	StatusCanceled MessageStatus = "canceled"
	StatusMissing  MessageStatus = "missing"
)

// ClaimedMessage is the only public capability that can ack or retry an inflight message.
type ClaimedMessage struct {
	ID          MessageID
	Body        string
	Attempt     int
	PublishedAt time.Time
	VisibleAt   time.Time
	Deadline    time.Time

	ack             func(context.Context) error
	nack            func(context.Context, time.Duration) error
	touch           func(context.Context, time.Duration) error
	cancelRequested func(context.Context) (bool, error)
}

type MessageState struct {
	ID              MessageID
	Status          MessageStatus
	Attempt         int
	PublishedAt     time.Time
	VisibleAt       time.Time
	Deadline        time.Time
	CancelRequested bool
	DeadAt          time.Time
}

type Stats struct {
	Ready    int64
	Inflight int64
	Dead     int64
	Total    int64
}

type DeadMessage struct {
	ID              MessageID
	Body            string
	Attempt         int
	PublishedAt     time.Time
	DeadAt          time.Time
	CancelRequested bool
}

type DeadListResult struct {
	Messages   []DeadMessage
	NextCursor string
	HasMore    bool
}

func newClaimedMessage(
	id, body string,
	attempt int,
	publishedAt, visibleAt, deadline time.Time,
	ack func(context.Context) error,
	nack func(context.Context, time.Duration) error,
	touch func(context.Context, time.Duration) error,
	cancelRequested func(context.Context) (bool, error),
) *ClaimedMessage {
	return &ClaimedMessage{
		ID:              id,
		Body:            body,
		Attempt:         attempt,
		PublishedAt:     publishedAt,
		VisibleAt:       visibleAt,
		Deadline:        deadline,
		ack:             ack,
		nack:            nack,
		touch:           touch,
		cancelRequested: cancelRequested,
	}
}

func (m *ClaimedMessage) Ack(ctx context.Context) error {
	if m == nil || m.ack == nil {
		return ErrInvalidMessage
	}
	return m.ack(ctx)
}

func (m *ClaimedMessage) Nack(ctx context.Context, delay time.Duration) error {
	if m == nil || m.nack == nil {
		return ErrInvalidMessage
	}
	return m.nack(ctx, delay)
}

func (m *ClaimedMessage) Touch(ctx context.Context, ttl time.Duration) error {
	if m == nil || m.touch == nil {
		return ErrInvalidMessage
	}
	return m.touch(ctx, ttl)
}

func (m *ClaimedMessage) CancelRequested(ctx context.Context) (bool, error) {
	if m == nil || m.cancelRequested == nil {
		return false, ErrInvalidMessage
	}
	return m.cancelRequested(ctx)
}

func NewQueueFromURL(s string) (CloserNamespace, error) {
	parse, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	switch parse.Scheme {
	case "memory", "mem":
		return NewMemoryQueue(), nil
	case "etcd":
		return NewEtcdQueueFromURL(s)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

var (
	ErrQueueClosed        = errors.New("queue closed")
	ErrInvalidMessage     = errors.New("invalid claimed message")
	ErrInvalidTopic       = errors.New("invalid topic")
	ErrInvalidAckTimeout  = errors.New("ack timeout must be greater than 0")
	ErrInvalidDelay       = errors.New("delay must be greater than or equal to 0")
	ErrInvalidLimit       = errors.New("limit must be greater than 0")
	ErrClaimMismatch      = errors.New("claim mismatch")
	ErrMessageNotFound    = errors.New("message not found")
	ErrDeadNotFound       = errors.New("dead message not found")
	ErrInvalidState       = errors.New("invalid message state")
	ErrEntropyUnavailable = errors.New("secure random source unavailable")
)

func normalizeConfig(config Config) Config {
	if config.DefaultAckTimeout <= 0 {
		config.DefaultAckTimeout = 30 * time.Second
	}
	if config.RequeueInterval <= 0 {
		config.RequeueInterval = 25 * time.Millisecond
	}
	return config
}

func normalizeConsumeOptions(config Config, opts *ConsumeOptions) (ConsumeOptions, error) {
	normalized := ConsumeOptions{AckTimeout: config.DefaultAckTimeout}
	if opts != nil && opts.AckTimeout != 0 {
		normalized.AckTimeout = opts.AckTimeout
	}
	if normalized.AckTimeout <= 0 {
		return ConsumeOptions{}, ErrInvalidAckTimeout
	}
	return normalized, nil
}

func normalizeListOptions(opts *ListOptions) ListOptions {
	normalized := ListOptions{Limit: 100}
	if opts != nil {
		normalized = *opts
		if normalized.Limit <= 0 {
			normalized.Limit = 100
		}
	}
	return normalized
}

func normalizePaths(paths ...string) []string {
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		path = strings.Trim(path, "/")
		if path == "" {
			continue
		}
		keys = append(keys, path)
	}
	return keys
}

func nextMessageID() (MessageID, error) {
	token, err := randomToken(16)
	if err != nil {
		return "", err
	}
	return "msg_" + token, nil
}

func nextClaimToken() (string, error) {
	token, err := randomToken(24)
	if err != nil {
		return "", err
	}
	return "clm_" + token, nil
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("%w: %v", ErrEntropyUnavailable, err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func claimTokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

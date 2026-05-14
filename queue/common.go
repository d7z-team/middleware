// Package queue provides namespaced at-least-once queue backends.
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
//
// Example:
//
//	ns, _ := NewQueueFromURL("memory://")
//	defer ns.Close()
//
//	producer := ns.Child("billing").Producer()
//	id, _ := producer.Publish(ctx, "jobs", "charge-user-1", &PublishOptions{
//		Delay:         time.Second,
//		MaxDeliveries: 5,
//	})
//	_ = id
type Producer interface {
	// Publish enqueues one message under the given topic and returns its message ID.
	Publish(ctx context.Context, topic, body string, opts *PublishOptions) (MessageID, error)
	// PublishBatch enqueues multiple messages under the same topic in one call.
	PublishBatch(ctx context.Context, topic string, messages []PublishRequest) ([]MessageID, error)
}

// Consumer claims visible messages from a queue namespace.
//
// Example:
//
//	ns, _ := NewQueueFromURL("memory://")
//	defer ns.Close()
//
//	consumer := ns.Child("billing").Consumer()
//	msg, _ := consumer.Consume(ctx, "jobs", nil)
//	if msg != nil {
//		_ = msg.Touch(ctx, time.Minute)
//		_ = msg.Ack(ctx)
//	}
type Consumer interface {
	// Consume claims the next visible message for the topic and returns a claimed capability.
	Consume(ctx context.Context, topic string, opts *ConsumeOptions) (*ClaimedMessage, error)
}

// Admin exposes queue inspection and management operations.
//
// Example:
//
//	ns, _ := NewQueueFromURL("memory://")
//	defer ns.Close()
//
//	admin := ns.Child("billing").Admin()
//	state, _ := admin.Get(ctx, "jobs", id)
//	stats, _ := admin.Count(ctx, "jobs")
//	_ = state.Status
//	_ = stats.Total
type Admin interface {
	// Get returns the persisted state for the specified message ID.
	Get(ctx context.Context, topic string, messageID MessageID) (*MessageState, error)
	// Peek returns up to limit currently queued messages without claiming them.
	Peek(ctx context.Context, topic string, limit int) ([]MessageState, error)
	// Count returns aggregate queue counters for the topic.
	Count(ctx context.Context, topic string) (*Stats, error)

	// GetDead returns one dead-letter message by ID.
	GetDead(ctx context.Context, topic string, messageID MessageID) (*DeadMessage, error)
	// ListDead returns a paginated list of dead-letter messages for the topic.
	ListDead(ctx context.Context, topic string, opts *ListOptions) (*DeadListResult, error)
	// CountDead returns the number of dead-letter messages for the topic.
	CountDead(ctx context.Context, topic string) (int64, error)
	// RequeueDead moves one dead-letter message back into the ready queue after an optional delay.
	RequeueDead(ctx context.Context, topic string, messageID MessageID, delay time.Duration) error
	// RequeueAllDead requeues up to limit dead-letter messages for the topic.
	RequeueAllDead(ctx context.Context, topic string, limit int, delay time.Duration) (int, error)
	// DeleteDead permanently removes one dead-letter message.
	DeleteDead(ctx context.Context, topic string, messageID MessageID) error

	// Cancel requests cancellation for the specified message.
	Cancel(ctx context.Context, topic string, messageID MessageID) error
	// Delete permanently removes a non-inflight message.
	Delete(ctx context.Context, topic string, messageID MessageID) error
}

// Namespace provides scoped producer, consumer, and admin views.
//
// Example:
//
//	ns, _ := NewQueueFromURL("memory://")
//	defer ns.Close()
//
//	child := ns.Child("team-a")
//	_, _ = child.Producer().Publish(ctx, "jobs", "payload", nil)
//	msg, _ := child.Consumer().Consume(ctx, "jobs", nil)
//	if msg != nil {
//		_ = msg.Ack(ctx)
//	}
type Namespace interface {
	// Child returns a namespaced view rooted under the provided path segments.
	Child(paths ...string) Namespace
	// Producer returns the publishing view for this namespace.
	Producer() Producer
	// Consumer returns the consuming view for this namespace.
	Consumer() Consumer
	// Admin returns the inspection and management view for this namespace.
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
//
// Example:
//
//	ns, _ := NewQueueFromURL("memory://")
//	defer ns.Close()
//
//	_, _ = ns.Producer().Publish(ctx, "jobs", "payload", nil)
//	msg, _ := ns.Consumer().Consume(ctx, "jobs", nil)
//	if msg != nil {
//		canceled, _ := msg.CancelRequested(ctx)
//		_ = canceled
//		_ = msg.Nack(ctx, time.Second)
//	}
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

// NewQueueFromURL creates a queue namespace from a connection URL.
//
// Example:
//
//	ns, _ := NewQueueFromURL("etcd://127.0.0.1:2379?prefix=jobs")
//	defer ns.Close()
//
//	tenant := ns.Child("tenant-a")
//	id, _ := tenant.Producer().Publish(ctx, "emails", "welcome-user", nil)
//	state, _ := tenant.Admin().Get(ctx, "emails", id)
//	_ = state.Status
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

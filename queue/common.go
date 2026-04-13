package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

// Queue defines an ackable at-least-once queue with management APIs.
type Queue interface {
	// Child returns a namespaced queue view under the given path segments.
	Child(paths ...string) Queue

	// Publish enqueues a single message and returns its message ID.
	Publish(ctx context.Context, topic, body string, opts *PublishOptions) (string, error)
	// PublishBatch enqueues multiple messages under the same topic.
	PublishBatch(ctx context.Context, topic string, messages []PublishRequest) ([]string, error)

	// Consume claims the next visible message and returns an ack handle.
	Consume(ctx context.Context, topic string, opts *ConsumeOptions) (*Message, error)

	// Get returns the current state of a message by ID.
	Get(ctx context.Context, topic, messageID string) (*MessageState, error)
	// Peek returns up to limit currently queued messages without consuming them.
	Peek(ctx context.Context, topic string, limit int) ([]MessageState, error)
	// Count returns aggregate queue counters for the topic.
	Count(ctx context.Context, topic string) (*Stats, error)

	// GetDead returns a dead-letter message by ID.
	GetDead(ctx context.Context, topic, messageID string) (*DeadMessage, error)
	// ListDead returns dead-letter messages ordered by the backend's dead-letter index.
	//
	// Pagination uses an opaque keyset cursor:
	// - pass ListOptions.Cursor as the NextCursor returned by the previous page
	// - do not parse, modify, or construct the cursor yourself
	// - cursor values are backend-specific and may encode internal index keys
	//
	// An empty cursor means "start from the beginning".
	ListDead(ctx context.Context, topic string, opts *ListOptions) (*DeadListResult, error)
	// CountDead returns the number of dead-letter messages for the topic.
	CountDead(ctx context.Context, topic string) (int64, error)
	// RequeueDead moves a dead-letter message back to the ready queue after an optional delay.
	RequeueDead(ctx context.Context, topic, messageID string, delay time.Duration) error
	// RequeueAllDead moves up to limit dead-letter messages back to the ready queue.
	RequeueAllDead(ctx context.Context, topic string, limit int, delay time.Duration) (int, error)
	// DeleteDead permanently removes a dead-letter message.
	DeleteDead(ctx context.Context, topic, messageID string) error

	// Cancel requests cancellation for a message.
	// Ready and dead messages transition to canceled immediately.
	// Inflight messages keep running until the consumer observes the cancel request.
	Cancel(ctx context.Context, topic, messageID string) error
	// Delete permanently removes a non-inflight message.
	Delete(ctx context.Context, topic, messageID string) error
}

// CloserQueue is a Queue that also owns resources that can be closed.
type CloserQueue interface {
	Queue
	io.Closer
}

// PublishOptions controls optional publish-time queue behavior.
type PublishOptions struct {
	// Delay postpones message visibility until now+Delay.
	Delay time.Duration
	// DedupKey coalesces repeated publishes according to backend-specific dedup behavior.
	DedupKey string
	// MaxDeliveries overrides the queue default dead-letter threshold for this message.
	MaxDeliveries int
}

// PublishRequest is one entry in a batch publish call.
type PublishRequest struct {
	Body string
	Opts *PublishOptions
}

// ConsumeOptions controls how a claimed message behaves while inflight.
type ConsumeOptions struct {
	// AckTimeout is the lease duration before an unacked message becomes eligible for redelivery.
	AckTimeout time.Duration
}

// ListOptions controls paginated listing operations.
type ListOptions struct {
	// Limit is the maximum number of results to return.
	Limit int
	// Cursor is an opaque keyset cursor returned by DeadListResult.NextCursor.
	// Callers must treat it as an unreadable continuation token and pass it back unchanged.
	Cursor string
}

// Config controls backend defaults such as ack timeout and retry scanning cadence.
type Config struct {
	DefaultAckTimeout time.Duration
	RequeueInterval   time.Duration
	MaxDeliveries     int
}

// MessageStatus describes the persisted lifecycle state of a message.
type MessageStatus string

const (
	StatusReady    MessageStatus = "ready"
	StatusInflight MessageStatus = "inflight"
	StatusDead     MessageStatus = "dead"
	StatusDone     MessageStatus = "done"
	StatusCanceled MessageStatus = "canceled"
	StatusMissing  MessageStatus = "missing"
)

// Message is a claimed message together with ack/retry control functions.
type Message struct {
	ID          string
	Body        string
	Attempt     int
	Receipt     string
	PublishedAt time.Time
	VisibleAt   time.Time
	Deadline    time.Time

	ack             func(context.Context) error
	nack            func(context.Context, time.Duration) error
	touch           func(context.Context, time.Duration) error
	cancelRequested func(context.Context) (bool, error)
}

// MessageState describes the current persisted state of a message.
type MessageState struct {
	ID              string
	Body            string
	Status          MessageStatus
	Attempt         int
	PublishedAt     time.Time
	VisibleAt       time.Time
	Deadline        time.Time
	Receipt         string
	CancelRequested bool
	DeadAt          time.Time
}

// Stats aggregates queue counts for one topic.
type Stats struct {
	Ready    int64
	Inflight int64
	Dead     int64
	Total    int64
}

// DeadMessage is a dead-letter message returned by dead-letter queries.
type DeadMessage struct {
	ID              string
	Body            string
	Attempt         int
	PublishedAt     time.Time
	DeadAt          time.Time
	CancelRequested bool
}

// DeadListResult is one page of dead-letter messages.
type DeadListResult struct {
	Messages []DeadMessage
	// NextCursor is an opaque keyset cursor for fetching the next page.
	// It is only meaningful when HasMore is true.
	NextCursor string
	// HasMore reports whether another page can be fetched with NextCursor.
	HasMore bool
}

func newClaimedMessage(
	id, body string,
	attempt int,
	receipt string,
	publishedAt, visibleAt, deadline time.Time,
	ack func(context.Context) error,
	nack func(context.Context, time.Duration) error,
	touch func(context.Context, time.Duration) error,
	cancelRequested func(context.Context) (bool, error),
) *Message {
	return &Message{
		ID:              id,
		Body:            body,
		Attempt:         attempt,
		Receipt:         receipt,
		PublishedAt:     publishedAt,
		VisibleAt:       visibleAt,
		Deadline:        deadline,
		ack:             ack,
		nack:            nack,
		touch:           touch,
		cancelRequested: cancelRequested,
	}
}

// Ack permanently marks the claimed message as completed.
func (m *Message) Ack(ctx context.Context) error {
	if m == nil || m.ack == nil {
		return ErrInvalidMessage
	}
	return m.ack(ctx)
}

// Nack releases the claimed message back to the queue after an optional delay.
func (m *Message) Nack(ctx context.Context, delay time.Duration) error {
	if m == nil || m.nack == nil {
		return ErrInvalidMessage
	}
	return m.nack(ctx, delay)
}

// Touch extends the inflight ack deadline for the claimed message.
func (m *Message) Touch(ctx context.Context, ttl time.Duration) error {
	if m == nil || m.touch == nil {
		return ErrInvalidMessage
	}
	return m.touch(ctx, ttl)
}

// CancelRequested reports whether a cancellation request has been recorded for this inflight message.
func (m *Message) CancelRequested(ctx context.Context) (bool, error) {
	if m == nil || m.cancelRequested == nil {
		return false, ErrInvalidMessage
	}
	return m.cancelRequested(ctx)
}

// NewQueueFromURL constructs a queue backend from a connection URL.
//
// Example:
//
//	q, err := NewQueueFromURL("memory://")
//	if err != nil {
//		return err
//	}
//	defer q.Close()
func NewQueueFromURL(s string) (CloserQueue, error) {
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
	ErrQueueClosed       = errors.New("queue closed")
	ErrInvalidMessage    = errors.New("invalid message")
	ErrInvalidTopic      = errors.New("invalid topic")
	ErrInvalidAckTimeout = errors.New("ack timeout must be greater than 0")
	ErrInvalidDelay      = errors.New("delay must be greater than or equal to 0")
	ErrInvalidLimit      = errors.New("limit must be greater than 0")
	ErrReceiptMismatch   = errors.New("receipt mismatch")
	ErrMessageNotFound   = errors.New("message not found")
	ErrDeadNotFound      = errors.New("dead message not found")
	ErrInvalidState      = errors.New("invalid message state")
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

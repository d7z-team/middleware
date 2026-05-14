// Package subscribe provides in-memory and etcd-backed pub/sub primitives.
package subscribe

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"gopkg.d7z.net/middleware/connects"
)

// Event is a published key/value notification.
type Event struct {
	Key      string
	Value    string
	Revision int64
}

// Subscription exposes event and error streams for one watch registration.
//
// Example:
//
//	sub, _ := NewSubscriberFromURL("memory://")
//	defer sub.Close()
//
//	stream, _ := sub.Subscribe(ctx, "topic")
//	defer stream.Close()
//
//	_ = sub.Publish(ctx, "topic", "hello")
//	event := <-stream.Events()
//	_ = event.Value
type Subscription interface {
	// Events returns the stream of published events delivered to this subscription.
	Events() <-chan Event
	// Errors returns asynchronous watch and transport errors for this subscription.
	Errors() <-chan error
	// Close stops the subscription and releases associated resources.
	Close() error
}

// Subscriber publishes events and creates subscriptions.
//
// Example:
//
//	sub, _ := NewSubscriberFromURL("memory://")
//	defer sub.Close()
//
//	child := sub.Child("tenant-a")
//	stream, _ := child.Subscribe(ctx, "events")
//	defer stream.Close()
//
//	_ = child.Publish(ctx, "events", "user-created")
//	event := <-stream.Events()
//	_ = event.Key
type Subscriber interface {
	// Child returns a subscriber scoped under the provided path segments.
	Child(paths ...string) Subscriber
	// Publish writes one event value to the specified key.
	Publish(ctx context.Context, key, data string) error
	// Subscribe registers a new subscription for the specified key.
	Subscribe(ctx context.Context, key string) (Subscription, error)
}

type CloserSubscriber interface {
	Subscriber
	io.Closer
}

type closerSubscriber struct {
	Subscriber
	closer func() error
}

func (c *closerSubscriber) Close() error {
	if c.closer == nil {
		return nil
	}
	return c.closer()
}

// NewSubscriberFromURL creates a subscriber from a connection URL.
//
// Example:
//
//	sub, _ := NewSubscriberFromURL("etcd://127.0.0.1:2379?prefix=events")
//	defer sub.Close()
//
//	stream, _ := sub.Subscribe(ctx, "orders")
//	defer stream.Close()
//	_ = sub.Publish(ctx, "orders", "placed")
func NewSubscriberFromURL(u string) (CloserSubscriber, error) {
	parse, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	switch parse.Scheme {
	case "memory", "mem":
		memory := NewMemorySubscriber()
		return &closerSubscriber{
			Subscriber: memory,
			closer:     memory.Close,
		}, nil
	case "etcd":
		etcd, err := connects.NewEtcd(parse)
		if err != nil {
			return nil, err
		}
		subscriber := NewEtcdSubscriber(etcd, parse.Query().Get("prefix"))
		return &closerSubscriber{
			Subscriber: subscriber,
			closer: func() error {
				if err := subscriber.Close(); err != nil {
					_ = etcd.Close()
					return err
				}
				return etcd.Close()
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

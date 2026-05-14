package subscribe

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"gopkg.d7z.net/middleware/connects"
)

type Event struct {
	Key      string
	Value    string
	Revision int64
}

type Subscription interface {
	Events() <-chan Event
	Errors() <-chan error
	Close() error
}

type Subscriber interface {
	Child(paths ...string) Subscriber
	Publish(ctx context.Context, key, data string) error
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

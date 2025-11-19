package subscribe

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"gopkg.d7z.net/middleware/connects"
)

type Subscriber interface {
	Child(prefix string) Subscriber
	Publish(ctx context.Context, key, data string) error
	Subscribe(ctx context.Context, key string) (<-chan string, error)
}

type CloserSubscriber interface {
	Subscriber
	io.Closer
}
type closerSubscriber struct {
	Subscriber
	io.Closer
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
			Closer:     memory,
		}, nil
	case "etcd":
		etcd, err := connects.NewEtcd(parse)
		if err != nil {
			return nil, err
		}
		return &closerSubscriber{
			Subscriber: NewEtcdSubscriber(etcd, parse.Query().Get("prefix")),
			Closer:     etcd,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

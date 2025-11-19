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
	Publish(ctx context.Context, key string, data string) error
	Subscribe(ctx context.Context, key string) (<-chan string, error)
}

type CloserSubscriber struct {
	Subscriber
	io.Closer
}

func NewSubscriberFromURL(u string) (*CloserSubscriber, error) {
	parse, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	switch parse.Scheme {
	case "memory", "mem":
		memory := NewMemorySubscriber()
		return &CloserSubscriber{
			Subscriber: memory,
			Closer:     memory,
		}, nil
	case "etcd":
		etcd, err := connects.NewEtcd(parse)
		if err != nil {
			return nil, err
		}
		return &CloserSubscriber{
			Subscriber: NewEtcdSubscriber(etcd, parse.Query().Get("prefix")),
			Closer:     etcd,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", parse.Scheme)
	}
}

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	kv2 "gopkg.d7z.net/middleware/kv"
)

type ID2KVI[ID comparable, Data any] interface {
	List(ctx context.Context, page uint64, size uint) (map[ID]Data, error)
	Get(ctx context.Context, id ID) (Data, error)
	Put(ctx context.Context, id ID, value Data) error
	PutIfNotExists(ctx context.Context, id ID, value Data) (bool, error)
	Delete(ctx context.Context, id ID) (bool, error)
}

type ID2KV[Data any] struct {
	kv     kv2.PagedKV
	prefix string
}

func NewID2KV[Data any](kv kv2.PagedKV, prefix string) ID2KVI[string, Data] {
	return &ID2KV[Data]{
		kv:     kv,
		prefix: prefix,
	}
}

func (d *ID2KV[Data]) List(ctx context.Context, page uint64, size uint) (map[string]Data, error) {
	listPage, err := d.kv.ListPage(ctx, d.prefix, page, size)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]Data, size)
	for k, v := range listPage {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		key := strings.TrimPrefix(k, d.prefix)
		var value Data
		err = json.Unmarshal([]byte(v), &value)
		if err != nil {
			return nil, err
		}
		ret[key] = value
	}
	return ret, nil
}

func (d *ID2KV[Data]) Get(ctx context.Context, id string) (Data, error) {
	key := fmt.Sprintf("%s%s", d.prefix, id)
	get, err := d.kv.Get(ctx, key)
	var data Data
	if err != nil {
		return data, err
	}
	err = json.Unmarshal([]byte(get), &data)
	if err != nil {
		return data, err
	}
	return data, nil
}

func (d *ID2KV[Data]) Put(ctx context.Context, id string, value Data) error {
	key := fmt.Sprintf("%s%s", d.prefix, id)
	data, err := json.Marshal(&value)
	if err != nil {
		return err
	}
	return d.kv.Put(ctx, key, string(data), kv2.TTLKeep)
}

func (d *ID2KV[Data]) PutIfNotExists(ctx context.Context, id string, value Data) (bool, error) {
	key := fmt.Sprintf("%s%s", d.prefix, id)
	data, err := json.Marshal(&value)
	if err != nil {
		return false, err
	}
	return d.kv.PutIfNotExists(ctx, key, string(data), kv2.TTLKeep)
}

func (d *ID2KV[Data]) Delete(ctx context.Context, id string) (bool, error) {
	return d.kv.Delete(ctx, fmt.Sprintf("%s%s", d.prefix, id))
}

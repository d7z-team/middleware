package tools

import (
	"context"
	"encoding/json"

	"gopkg.d7z.net/middleware/kv"
)

type IDValue[ID comparable, Data any] struct {
	ID   ID
	Data Data
}

type ID2KVI[ID comparable, Data any] interface {
	List(ctx context.Context, page uint64, size uint) ([]IDValue[ID, Data], error)
	Get(ctx context.Context, id ID) (Data, error)
	Put(ctx context.Context, id ID, value Data) error
	PutIfNotExists(ctx context.Context, id ID, value Data) (bool, error)
	Delete(ctx context.Context, id ID) (bool, error)
}

type ID2KV[Data any] struct {
	kv kv.KV
}

func NewID2KV[Data any](kv kv.KV, prefix string) ID2KVI[string, Data] {
	return &ID2KV[Data]{
		kv: kv.Child(prefix),
	}
}

func (d *ID2KV[Data]) List(ctx context.Context, page uint64, size uint) ([]IDValue[string, Data], error) {
	listPage, err := d.kv.ListPage(ctx, "", page, size)
	if err != nil {
		return nil, err
	}
	ret := make([]IDValue[string, Data], 0, len(listPage))
	for _, p := range listPage {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var value Data
		err = json.Unmarshal([]byte(p.Value), &value)
		if err != nil {
			return nil, err
		}
		ret = append(ret, IDValue[string, Data]{ID: p.Key, Data: value})
	}
	return ret, nil
}

func (d *ID2KV[Data]) Get(ctx context.Context, id string) (Data, error) {
	get, err := d.kv.Get(ctx, id)
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
	data, err := json.Marshal(&value)
	if err != nil {
		return err
	}
	return d.kv.Put(ctx, id, string(data), kv.TTLKeep)
}

func (d *ID2KV[Data]) PutIfNotExists(ctx context.Context, id string, value Data) (bool, error) {
	data, err := json.Marshal(&value)
	if err != nil {
		return false, err
	}
	return d.kv.PutIfNotExists(ctx, id, string(data), kv.TTLKeep)
}

func (d *ID2KV[Data]) Delete(ctx context.Context, id string) (bool, error) {
	return d.kv.Delete(ctx, id)
}

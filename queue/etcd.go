package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.d7z.net/middleware/connects"
)

const (
	etcdConsumeScanLimit = 64
	etcdReapScanLimit    = 128
	etcdCASRetryLimit    = 8
	etcdRetryBackoff     = 10 * time.Millisecond
)

type EtcdQueue struct {
	client *clientv3.Client
	prefix string
	closer func() error
	config Config
	mu     sync.RWMutex
	closed bool
}

type etcdStoredMessage struct {
	ID              string        `json:"id"`
	Body            string        `json:"body"`
	DedupKey        string        `json:"dedup_key,omitempty"`
	Attempt         int           `json:"attempt"`
	MaxDeliveries   int           `json:"max_deliveries"`
	Status          MessageStatus `json:"status"`
	PublishedAt     time.Time     `json:"published_at"`
	VisibleAt       time.Time     `json:"visible_at"`
	Deadline        time.Time     `json:"deadline"`
	DeadAt          time.Time     `json:"dead_at"`
	Receipt         string        `json:"receipt,omitempty"`
	CancelRequested bool          `json:"cancel_requested"`
	Sequence        uint64        `json:"sequence"`
	ReadyKey        string        `json:"ready_key,omitempty"`
	InflightKey     string        `json:"inflight_key,omitempty"`
	DeadKey         string        `json:"dead_key,omitempty"`
}

var etcdSequence uint64

func NewEtcdQueue(client *clientv3.Client, prefix string) *EtcdQueue {
	return newEtcdQueue(client, prefix, client.Close, Config{})
}

func newEtcdQueue(client *clientv3.Client, prefix string, closer func() error, config Config) *EtcdQueue {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &EtcdQueue{
		client: client,
		prefix: prefix,
		closer: closer,
		config: normalizeConfig(config),
	}
}

func (e *EtcdQueue) Child(paths ...string) Queue {
	keys := normalizePaths(paths...)
	if len(keys) == 0 {
		return e
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return &EtcdQueue{
		client: e.client,
		prefix: e.prefix + strings.Join(keys, "/") + "/",
		config: e.config,
		closed: e.closed,
	}
}

func (e *EtcdQueue) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	if e.closer == nil {
		return nil
	}
	return e.closer()
}

func (e *EtcdQueue) Publish(ctx context.Context, topic, body string, opts *PublishOptions) (string, error) {
	fullTopic, publishOpts, err := e.preparePublish(ctx, topic, opts)
	if err != nil {
		return "", err
	}

	now := time.Now()
	message := &etcdStoredMessage{
		ID:            nextMessageID(),
		Body:          body,
		DedupKey:      publishOpts.DedupKey,
		MaxDeliveries: publishOpts.MaxDeliveries,
		Status:        StatusReady,
		PublishedAt:   now,
	}
	e.markReady(fullTopic, message, now.Add(publishOpts.Delay))
	return e.publishOne(ctx, fullTopic, message)
}

func (e *EtcdQueue) PublishBatch(ctx context.Context, topic string, requests []PublishRequest) ([]string, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return []string{}, nil
	}

	now := time.Now()
	ids := make([]string, 0, len(requests))
	batchedOps := make([]clientv3.Op, 0, len(requests)*2)
	batchedCmp := make([]clientv3.Cmp, 0, len(requests))
	fallback := make([]*etcdStoredMessage, 0, len(requests))

	for _, request := range requests {
		opts, err := e.normalizePublishOptions(request.Opts)
		if err != nil {
			return nil, err
		}

		message := &etcdStoredMessage{
			ID:            nextMessageID(),
			Body:          request.Body,
			DedupKey:      opts.DedupKey,
			MaxDeliveries: opts.MaxDeliveries,
			Status:        StatusReady,
			PublishedAt:   now,
		}
		e.markReady(fullTopic, message, now.Add(opts.Delay))
		ids = append(ids, message.ID)

		if message.DedupKey != "" {
			fallback = append(fallback, message)
			continue
		}

		raw, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		batchedCmp = append(batchedCmp, clientv3.Compare(clientv3.Version(e.messageKey(fullTopic, message.ID)), "=", 0))
		batchedOps = append(batchedOps,
			clientv3.OpPut(e.messageKey(fullTopic, message.ID), string(raw)),
			clientv3.OpPut(message.ReadyKey, message.ID),
		)
	}

	if len(batchedOps) > 0 {
		_, err := e.client.Txn(ctx).If(batchedCmp...).Then(batchedOps...).Commit()
		if err != nil {
			return nil, err
		}
	}

	for _, message := range fallback {
		id, err := e.publishOne(ctx, fullTopic, message)
		if err != nil {
			return nil, err
		}
		for i, candidate := range ids {
			if candidate == message.ID {
				ids[i] = id
				break
			}
		}
	}

	return ids, nil
}

func (e *EtcdQueue) Consume(ctx context.Context, topic string, opts *ConsumeOptions) (*Message, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	consumeOpts, err := normalizeConsumeOptions(e.config, opts)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := e.reapExpired(ctx, fullTopic, time.Now()); err != nil {
			return nil, err
		}

		msg, wait, revision, err := e.tryConsume(ctx, fullTopic, consumeOpts.AckTimeout, time.Now())
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
		if err := e.waitForReady(ctx, fullTopic, revision, wait); err != nil {
			return nil, err
		}
	}
}

func (e *EtcdQueue) Get(ctx context.Context, topic, messageID string) (*MessageState, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())

	msg, _, err := e.getMessage(ctx, fullTopic, messageID)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return &MessageState{ID: messageID, Status: StatusMissing}, nil
	}
	state := stateFromEtcd(msg)
	return &state, nil
}

func (e *EtcdQueue) Peek(ctx context.Context, topic string, limit int) ([]MessageState, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())

	ready, _, err := e.listIndexedMessages(ctx, fullTopic, e.readyPrefix(fullTopic), limit, "")
	if err != nil {
		return nil, err
	}
	result := make([]MessageState, 0, len(ready))
	for _, msg := range ready {
		result = append(result, stateFromEtcd(msg))
	}
	return result, nil
}

func (e *EtcdQueue) Count(ctx context.Context, topic string) (*Stats, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())

	ready, err := e.countPrefix(ctx, e.readyPrefix(fullTopic))
	if err != nil {
		return nil, err
	}
	inflight, err := e.countPrefix(ctx, e.inflightPrefix(fullTopic))
	if err != nil {
		return nil, err
	}
	dead, err := e.countPrefix(ctx, e.deadPrefix(fullTopic))
	if err != nil {
		return nil, err
	}
	return &Stats{
		Ready:    ready,
		Inflight: inflight,
		Dead:     dead,
		Total:    ready + inflight + dead,
	}, nil
}

func (e *EtcdQueue) GetDead(ctx context.Context, topic, messageID string) (*DeadMessage, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())

	msg, _, err := e.getMessage(ctx, fullTopic, messageID)
	if err != nil {
		return nil, err
	}
	if msg == nil || msg.Status != StatusDead {
		return nil, ErrDeadNotFound
	}
	dead := deadFromEtcd(msg)
	return &dead, nil
}

func (e *EtcdQueue) ListDead(ctx context.Context, topic string, opts *ListOptions) (*DeadListResult, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return nil, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())

	listOpts := normalizeListOptions(opts)
	dead, nextCursor, err := e.listIndexedMessages(ctx, fullTopic, e.deadPrefix(fullTopic), listOpts.Limit, e.normalizeCursor(e.deadPrefix(fullTopic), listOpts.Cursor))
	if err != nil {
		return nil, err
	}
	result := &DeadListResult{Messages: make([]DeadMessage, 0, len(dead))}
	for _, msg := range dead {
		result.Messages = append(result.Messages, deadFromEtcd(msg))
	}
	if nextCursor != "" {
		result.HasMore = true
		result.NextCursor = nextCursor
	}
	return result, nil
}

func (e *EtcdQueue) CountDead(ctx context.Context, topic string) (int64, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return 0, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return 0, err
	}
	_ = e.reapExpired(ctx, fullTopic, time.Now())
	return e.countPrefix(ctx, e.deadPrefix(fullTopic))
}

func (e *EtcdQueue) RequeueDead(ctx context.Context, topic, messageID string, delay time.Duration) error {
	if delay < 0 {
		return ErrInvalidDelay
	}
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return err
	}
	return e.updateMessage(ctx, fullTopic, messageID, func(msg *etcdStoredMessage) error {
		if msg.Status != StatusDead {
			return ErrDeadNotFound
		}
		e.markReady(fullTopic, msg, time.Now().Add(delay))
		return nil
	})
}

func (e *EtcdQueue) RequeueAllDead(ctx context.Context, topic string, limit int, delay time.Duration) (int, error) {
	if delay < 0 {
		return 0, ErrInvalidDelay
	}
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return 0, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return 0, err
	}
	if limit <= 0 {
		limit = 1000
	}

	dead, _, err := e.listIndexedMessages(ctx, fullTopic, e.deadPrefix(fullTopic), limit, "")
	if err != nil {
		return 0, err
	}
	done := 0
	now := time.Now()
	for _, msg := range dead {
		err = e.updateMessage(ctx, fullTopic, msg.ID, func(current *etcdStoredMessage) error {
			if current.Status != StatusDead {
				return ErrDeadNotFound
			}
			e.markReady(fullTopic, current, now.Add(delay))
			return nil
		})
		if err == nil {
			done++
			continue
		}
		if errors.Is(err, ErrDeadNotFound) || errors.Is(err, ErrMessageNotFound) {
			continue
		}
		return done, err
	}
	return done, nil
}

func (e *EtcdQueue) DeleteDead(ctx context.Context, topic, messageID string) error {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return err
	}
	return e.deleteByStatus(ctx, fullTopic, messageID, StatusDead, ErrDeadNotFound)
}

func (e *EtcdQueue) Cancel(ctx context.Context, topic, messageID string) error {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return err
	}
	return e.updateMessage(ctx, fullTopic, messageID, func(msg *etcdStoredMessage) error {
		switch msg.Status {
		case StatusReady, StatusDead:
			e.markCanceled(msg)
			return nil
		case StatusInflight:
			msg.CancelRequested = true
			return nil
		case StatusDone, StatusCanceled:
			return ErrInvalidState
		default:
			return ErrInvalidState
		}
	})
}

func (e *EtcdQueue) Delete(ctx context.Context, topic, messageID string) error {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return err
	}

	msg, raw, err := e.getMessage(ctx, fullTopic, messageID)
	if err != nil {
		return err
	}
	if msg == nil {
		return ErrMessageNotFound
	}
	if msg.Status == StatusInflight {
		return ErrInvalidState
	}
	return e.deleteMessageTx(ctx, fullTopic, msg, raw)
}

func (e *EtcdQueue) preparePublish(ctx context.Context, topic string, opts *PublishOptions) (string, PublishOptions, error) {
	fullTopic, err := e.buildTopic(topic)
	if err != nil {
		return "", PublishOptions{}, err
	}
	if err := e.ensureOpen(ctx); err != nil {
		return "", PublishOptions{}, err
	}
	publishOpts, err := e.normalizePublishOptions(opts)
	if err != nil {
		return "", PublishOptions{}, err
	}
	return fullTopic, publishOpts, nil
}

func (e *EtcdQueue) ensureOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrQueueClosed
	}
	return nil
}

func (e *EtcdQueue) buildTopic(topic string) (string, error) {
	topic = strings.Trim(topic, "/")
	if topic == "" {
		return "", ErrInvalidTopic
	}
	if e.prefix == "" {
		return topic, nil
	}
	return e.prefix + topic, nil
}

func (e *EtcdQueue) normalizePublishOptions(opts *PublishOptions) (PublishOptions, error) {
	normalized := PublishOptions{}
	if opts != nil {
		normalized = *opts
	}
	if normalized.Delay < 0 {
		return PublishOptions{}, ErrInvalidDelay
	}
	if normalized.MaxDeliveries < 0 {
		return PublishOptions{}, ErrInvalidState
	}
	if normalized.MaxDeliveries == 0 {
		normalized.MaxDeliveries = e.config.MaxDeliveries
	}
	return normalized, nil
}

func (e *EtcdQueue) publishOne(ctx context.Context, topic string, msg *etcdStoredMessage) (string, error) {
	msgKey := e.messageKey(topic, msg.ID)
	raw, err := json.Marshal(msg)
	if err != nil {
		return "", err
	}

	if msg.DedupKey == "" {
		_, err = e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.Version(msgKey), "=", 0)).
			Then(
				clientv3.OpPut(msgKey, string(raw)),
				clientv3.OpPut(msg.ReadyKey, msg.ID),
			).
			Commit()
		return msg.ID, err
	}

	dedupKey := e.dedupKey(topic, msg.DedupKey)
	txnResp, err := e.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Version(dedupKey), "=", 0),
			clientv3.Compare(clientv3.Version(msgKey), "=", 0),
		).
		Then(
			clientv3.OpPut(msgKey, string(raw)),
			clientv3.OpPut(msg.ReadyKey, msg.ID),
			clientv3.OpPut(dedupKey, msg.ID),
		).
		Else(clientv3.OpGet(dedupKey)).
		Commit()
	if err != nil {
		return "", err
	}
	if txnResp.Succeeded {
		return msg.ID, nil
	}
	if len(txnResp.Responses) > 0 && txnResp.Responses[0].GetResponseRange() != nil && len(txnResp.Responses[0].GetResponseRange().Kvs) > 0 {
		return string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value), nil
	}
	return msg.ID, nil
}

func (e *EtcdQueue) tryConsume(ctx context.Context, topic string, ackTimeout time.Duration, now time.Time) (*Message, time.Duration, int64, error) {
	resp, err := e.client.Get(
		ctx,
		e.readyPrefix(topic),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(etcdConsumeScanLimit),
	)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(resp.Kvs) == 0 {
		return nil, 0, resp.Header.Revision, nil
	}

	wait := time.Duration(0)
	for _, kv := range resp.Kvs {
		visibleAt, messageID := parseIndexedKey(string(kv.Key))
		if visibleAt.After(now) {
			wait = visibleAt.Sub(now)
			break
		}

		msg, raw, err := e.getMessage(ctx, topic, messageID)
		if err != nil {
			return nil, 0, 0, err
		}
		if msg == nil || msg.Status != StatusReady || msg.ReadyKey != string(kv.Key) {
			_, _ = e.client.Delete(ctx, string(kv.Key))
			continue
		}

		msg.Attempt++
		msg.Status = StatusInflight
		msg.Receipt = nextReceiptID()
		msg.CancelRequested = false
		msg.ReadyKey = ""
		msg.InflightKey = e.inflightKey(topic, now.Add(ackTimeout), nextEtcdSequence(), msg.ID)
		msg.Deadline = now.Add(ackTimeout)

		updatedRaw, err := json.Marshal(msg)
		if err != nil {
			return nil, 0, 0, err
		}
		txnResp, err := e.client.Txn(ctx).
			If(
				clientv3.Compare(clientv3.Version(string(kv.Key)), ">", 0),
				clientv3.Compare(clientv3.Value(e.messageKey(topic, messageID)), "=", string(raw)),
			).
			Then(
				clientv3.OpPut(e.messageKey(topic, messageID), string(updatedRaw)),
				clientv3.OpDelete(string(kv.Key)),
				clientv3.OpPut(msg.InflightKey, msg.ID),
			).
			Commit()
		if err != nil {
			return nil, 0, 0, err
		}
		if !txnResp.Succeeded {
			continue
		}

		receipt := msg.Receipt
		return newClaimedMessage(
			msg.ID,
			msg.Body,
			msg.Attempt,
			receipt,
			msg.PublishedAt,
			msg.VisibleAt,
			msg.Deadline,
			func(callCtx context.Context) error {
				return e.ack(callCtx, topic, msg.ID, receipt)
			},
			func(callCtx context.Context, delay time.Duration) error {
				return e.nack(callCtx, topic, msg.ID, receipt, delay)
			},
			func(callCtx context.Context, ttl time.Duration) error {
				return e.touch(callCtx, topic, msg.ID, receipt, ttl)
			},
			func(callCtx context.Context) (bool, error) {
				return e.cancelRequested(callCtx, topic, msg.ID, receipt)
			},
		), 0, resp.Header.Revision, nil
	}
	return nil, wait, resp.Header.Revision, nil
}

func (e *EtcdQueue) waitForReady(ctx context.Context, topic string, revision int64, wait time.Duration) error {
	if wait <= 0 {
		wait = e.config.RequeueInterval
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchCh := e.client.Watch(watchCtx, e.readyPrefix(topic), clientv3.WithPrefix(), clientv3.WithRev(revision+1))
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case _, ok := <-watchCh:
		if !ok {
			return nil
		}
		return nil
	}
}

func (e *EtcdQueue) reapExpired(ctx context.Context, topic string, now time.Time) error {
	for {
		resp, err := e.client.Get(
			ctx,
			e.inflightPrefix(topic),
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			clientv3.WithLimit(etcdReapScanLimit),
		)
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		processed := 0
		for _, kv := range resp.Kvs {
			deadline, messageID := parseIndexedKey(string(kv.Key))
			if deadline.After(now) {
				return nil
			}

			msg, _, err := e.getMessage(ctx, topic, messageID)
			if err != nil {
				return err
			}
			if msg == nil || msg.Status != StatusInflight || msg.InflightKey != string(kv.Key) {
				_, _ = e.client.Delete(ctx, string(kv.Key))
				continue
			}

			err = e.updateMessage(ctx, topic, messageID, func(current *etcdStoredMessage) error {
				if current.Status != StatusInflight || current.InflightKey != string(kv.Key) || now.Before(current.Deadline) {
					return ErrReceiptMismatch
				}
				if current.MaxDeliveries > 0 && current.Attempt >= current.MaxDeliveries {
					e.markDead(current, now, topic)
					return nil
				}
				e.markReady(topic, current, now)
				return nil
			})
			if err != nil && !errors.Is(err, ErrReceiptMismatch) && !errors.Is(err, ErrMessageNotFound) {
				return err
			}
			processed++
		}

		if processed < etcdReapScanLimit {
			return nil
		}
	}
}

func (e *EtcdQueue) ack(ctx context.Context, topic, messageID, receipt string) error {
	return e.updateInflight(ctx, topic, messageID, receipt, func(msg *etcdStoredMessage) error {
		e.markDone(msg)
		return nil
	})
}

func (e *EtcdQueue) nack(ctx context.Context, topic, messageID, receipt string, delay time.Duration) error {
	if delay < 0 {
		return ErrInvalidDelay
	}
	return e.updateInflight(ctx, topic, messageID, receipt, func(msg *etcdStoredMessage) error {
		e.markReady(topic, msg, time.Now().Add(delay))
		return nil
	})
}

func (e *EtcdQueue) touch(ctx context.Context, topic, messageID, receipt string, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidAckTimeout
	}
	return e.updateInflight(ctx, topic, messageID, receipt, func(msg *etcdStoredMessage) error {
		msg.Deadline = time.Now().Add(ttl)
		msg.InflightKey = e.inflightKey(topic, msg.Deadline, nextEtcdSequence(), msg.ID)
		return nil
	})
}

func (e *EtcdQueue) cancelRequested(ctx context.Context, topic, messageID, receipt string) (bool, error) {
	msg, _, err := e.getMessage(ctx, topic, messageID)
	if err != nil {
		return false, err
	}
	if msg == nil || msg.Status != StatusInflight || msg.Receipt != receipt {
		return false, ErrReceiptMismatch
	}
	return msg.CancelRequested, nil
}

func (e *EtcdQueue) updateInflight(ctx context.Context, topic, messageID, receipt string, mutate func(*etcdStoredMessage) error) error {
	return e.updateMessage(ctx, topic, messageID, func(msg *etcdStoredMessage) error {
		if msg.Status != StatusInflight || msg.Receipt != receipt {
			return ErrReceiptMismatch
		}
		return mutate(msg)
	})
}

func (e *EtcdQueue) updateMessage(ctx context.Context, topic, messageID string, mutate func(*etcdStoredMessage) error) error {
	var lastErr error
	for attempt := 0; attempt < etcdCASRetryLimit; attempt++ {
		msg, raw, err := e.getMessage(ctx, topic, messageID)
		if err != nil {
			return err
		}
		if msg == nil {
			return ErrMessageNotFound
		}

		prev := *msg
		if err := mutate(msg); err != nil {
			return err
		}
		updatedRaw, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		ops := []clientv3.Op{clientv3.OpPut(e.messageKey(topic, messageID), string(updatedRaw))}
		ops = append(ops, e.indexTransitionOps(&prev, msg)...)
		if prev.DedupKey != "" && (msg.Status == StatusDone || msg.Status == StatusCanceled || msg.DedupKey == "") {
			ops = append(ops, clientv3.OpDelete(e.dedupKey(topic, prev.DedupKey)))
		}

		txnResp, err := e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.Value(e.messageKey(topic, messageID)), "=", string(raw))).
			Then(ops...).
			Commit()
		if err != nil {
			return err
		}
		if txnResp.Succeeded {
			return nil
		}
		lastErr = ErrMessageNotFound
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(etcdRetryBackoff):
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return ErrMessageNotFound
}

func (e *EtcdQueue) deleteByStatus(ctx context.Context, topic, messageID string, status MessageStatus, errNotFound error) error {
	msg, raw, err := e.getMessage(ctx, topic, messageID)
	if err != nil {
		return err
	}
	if msg == nil || msg.Status != status {
		return errNotFound
	}
	return e.deleteMessageTx(ctx, topic, msg, raw)
}

func (e *EtcdQueue) deleteMessageTx(ctx context.Context, topic string, msg *etcdStoredMessage, raw []byte) error {
	ops := []clientv3.Op{clientv3.OpDelete(e.messageKey(topic, msg.ID))}
	ops = append(ops, e.deleteIndexes(msg)...)
	if msg.DedupKey != "" {
		ops = append(ops, clientv3.OpDelete(e.dedupKey(topic, msg.DedupKey)))
	}
	txnResp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(e.messageKey(topic, msg.ID)), "=", string(raw))).
		Then(ops...).
		Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return ErrMessageNotFound
	}
	return nil
}

func (e *EtcdQueue) getMessage(ctx context.Context, topic, messageID string) (*etcdStoredMessage, []byte, error) {
	resp, err := e.client.Get(ctx, e.messageKey(topic, messageID), clientv3.WithLimit(1))
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil, nil
	}
	msg := &etcdStoredMessage{}
	if err := json.Unmarshal(resp.Kvs[0].Value, msg); err != nil {
		return nil, nil, err
	}
	return msg, resp.Kvs[0].Value, nil
}

func (e *EtcdQueue) listIndexedMessages(ctx context.Context, topic, prefix string, limit int, cursor string) ([]*etcdStoredMessage, string, error) {
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(int64(limit + 2)),
	}
	key := prefix
	if cursor != "" {
		key = cursor
		opts = append(opts, clientv3.WithFromKey())
	} else {
		opts = append(opts, clientv3.WithPrefix())
	}

	resp, err := e.client.Get(ctx, key, opts...)
	if err != nil {
		return nil, "", err
	}

	msgs := make([]*etcdStoredMessage, 0, limit)
	keys := make([]string, 0, limit+1)
	for _, kv := range resp.Kvs {
		if !strings.HasPrefix(string(kv.Key), prefix) {
			break
		}
		if cursor != "" && string(kv.Key) == cursor {
			continue
		}
		_, messageID := parseIndexedKey(string(kv.Key))
		msg, _, err := e.getMessage(ctx, topic, messageID)
		if err != nil {
			return nil, "", err
		}
		if msg == nil {
			continue
		}
		keys = append(keys, string(kv.Key))
		if len(msgs) == limit {
			break
		}
		msgs = append(msgs, msg)
	}
	if len(keys) <= limit {
		return msgs, "", nil
	}
	return msgs, keys[limit-1], nil
}

func (e *EtcdQueue) countPrefix(ctx context.Context, prefix string) (int64, error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (e *EtcdQueue) indexTransitionOps(prev, current *etcdStoredMessage) []clientv3.Op {
	ops := make([]clientv3.Op, 0, 6)
	for _, key := range []struct {
		old string
		new string
		val string
	}{
		{old: prev.ReadyKey, new: current.ReadyKey, val: current.ID},
		{old: prev.InflightKey, new: current.InflightKey, val: current.ID},
		{old: prev.DeadKey, new: current.DeadKey, val: current.ID},
	} {
		if key.old != "" && key.old != key.new {
			ops = append(ops, clientv3.OpDelete(key.old))
		}
		if key.new != "" && key.old != key.new {
			ops = append(ops, clientv3.OpPut(key.new, key.val))
		}
	}
	return ops
}

func (e *EtcdQueue) deleteIndexes(msg *etcdStoredMessage) []clientv3.Op {
	ops := make([]clientv3.Op, 0, 3)
	for _, key := range []string{msg.ReadyKey, msg.InflightKey, msg.DeadKey} {
		if key != "" {
			ops = append(ops, clientv3.OpDelete(key))
		}
	}
	return ops
}

func (e *EtcdQueue) markReady(topic string, msg *etcdStoredMessage, visibleAt time.Time) {
	msg.Status = StatusReady
	msg.VisibleAt = visibleAt
	msg.Deadline = time.Time{}
	msg.DeadAt = time.Time{}
	msg.Receipt = ""
	msg.CancelRequested = false
	msg.Sequence = nextEtcdSequence()
	msg.ReadyKey = e.readyKey(topic, visibleAt, msg.Sequence, msg.ID)
	msg.InflightKey = ""
	msg.DeadKey = ""
}

func (e *EtcdQueue) markDead(msg *etcdStoredMessage, deadAt time.Time, topic string) {
	msg.Status = StatusDead
	msg.DeadAt = deadAt
	msg.Deadline = time.Time{}
	msg.VisibleAt = time.Time{}
	msg.Receipt = ""
	msg.CancelRequested = false
	msg.ReadyKey = ""
	msg.InflightKey = ""
	msg.DeadKey = e.deadKey(topic, deadAt, nextEtcdSequence(), msg.ID)
}

func (e *EtcdQueue) markDone(msg *etcdStoredMessage) {
	msg.Status = StatusDone
	msg.VisibleAt = time.Time{}
	msg.Deadline = time.Time{}
	msg.DeadAt = time.Time{}
	msg.Receipt = ""
	msg.CancelRequested = false
	msg.ReadyKey = ""
	msg.InflightKey = ""
	msg.DeadKey = ""
}

func (e *EtcdQueue) markCanceled(msg *etcdStoredMessage) {
	msg.Status = StatusCanceled
	msg.VisibleAt = time.Time{}
	msg.Deadline = time.Time{}
	msg.DeadAt = time.Time{}
	msg.Receipt = ""
	msg.CancelRequested = true
	msg.ReadyKey = ""
	msg.InflightKey = ""
	msg.DeadKey = ""
}

func (e *EtcdQueue) topicPrefix(topic string) string {
	return path.Join("/", topic)
}

func (e *EtcdQueue) messagePrefix(topic string) string {
	return path.Join(e.topicPrefix(topic), "messages") + "/"
}

func (e *EtcdQueue) messageKey(topic, id string) string {
	return e.messagePrefix(topic) + id
}

func (e *EtcdQueue) readyPrefix(topic string) string {
	return path.Join(e.topicPrefix(topic), "ready") + "/"
}

func (e *EtcdQueue) inflightPrefix(topic string) string {
	return path.Join(e.topicPrefix(topic), "inflight") + "/"
}

func (e *EtcdQueue) deadPrefix(topic string) string {
	return path.Join(e.topicPrefix(topic), "dead") + "/"
}

func (e *EtcdQueue) readyKey(topic string, visibleAt time.Time, seq uint64, id string) string {
	return e.readyPrefix(topic) + indexedName(visibleAt, seq, id)
}

func (e *EtcdQueue) inflightKey(topic string, deadline time.Time, seq uint64, id string) string {
	return e.inflightPrefix(topic) + indexedName(deadline, seq, id)
}

func (e *EtcdQueue) deadKey(topic string, deadAt time.Time, seq uint64, id string) string {
	return e.deadPrefix(topic) + indexedName(deadAt, seq, id)
}

func (e *EtcdQueue) dedupKey(topic, dedup string) string {
	return path.Join(e.topicPrefix(topic), "dedup", base64.RawURLEncoding.EncodeToString([]byte(dedup)))
}

func (e *EtcdQueue) normalizeCursor(prefix, cursor string) string {
	if cursor == "" {
		return ""
	}
	if strings.HasPrefix(cursor, prefix) {
		return cursor
	}
	return ""
}

func indexedName(ts time.Time, seq uint64, id string) string {
	return fmt.Sprintf("%020d-%020d-%s", ts.UnixNano(), seq, id)
}

func parseIndexedKey(key string) (time.Time, string) {
	base := path.Base(key)
	parts := strings.SplitN(base, "-", 3)
	if len(parts) != 3 {
		return time.Time{}, ""
	}
	nanos, err := parseInt(parts[0])
	if err != nil {
		return time.Time{}, parts[2]
	}
	return time.Unix(0, nanos), parts[2]
}

func parseInt(v string) (int64, error) {
	var n int64
	for _, ch := range v {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid int")
		}
		n = n*10 + int64(ch-'0')
	}
	return n, nil
}

func stateFromEtcd(msg *etcdStoredMessage) MessageState {
	return MessageState{
		ID:              msg.ID,
		Body:            msg.Body,
		Status:          msg.Status,
		Attempt:         msg.Attempt,
		PublishedAt:     msg.PublishedAt,
		VisibleAt:       msg.VisibleAt,
		Deadline:        msg.Deadline,
		Receipt:         msg.Receipt,
		CancelRequested: msg.CancelRequested,
		DeadAt:          msg.DeadAt,
	}
}

func deadFromEtcd(msg *etcdStoredMessage) DeadMessage {
	return DeadMessage{
		ID:              msg.ID,
		Body:            msg.Body,
		Attempt:         msg.Attempt,
		PublishedAt:     msg.PublishedAt,
		DeadAt:          msg.DeadAt,
		CancelRequested: msg.CancelRequested,
	}
}

func nextEtcdSequence() uint64 {
	return atomic.AddUint64(&etcdSequence, 1)
}

func NewEtcdQueueFromURL(s string) (CloserQueue, error) {
	parse, err := url.Parse(s)
	if err != nil {
		return nil, err
	}
	client, err := connects.NewEtcd(parse)
	if err != nil {
		return nil, err
	}
	return newEtcdQueue(client, parse.Query().Get("prefix"), client.Close, Config{}), nil
}

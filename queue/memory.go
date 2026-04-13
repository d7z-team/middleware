package queue

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type memoryQueueState struct {
	mu     sync.Mutex
	topics map[string]*memoryTopic
	signal chan struct{}
	config Config
	closed bool
}

type memoryTopic struct {
	messages map[string]*memoryMessage
	ready    []*memoryReady
	dedup    map[string]string
}

type memoryMessage struct {
	ID              string
	Body            string
	DedupKey        string
	Attempt         int
	MaxDeliveries   int
	Status          MessageStatus
	PublishedAt     time.Time
	VisibleAt       time.Time
	Deadline        time.Time
	DeadAt          time.Time
	Receipt         string
	CancelRequested bool
	Sequence        uint64
}

type memoryReady struct {
	ID        string
	VisibleAt time.Time
	Sequence  uint64
}

type MemoryQueue struct {
	state       *memoryQueueState
	prefix      string
	parent      *MemoryQueue
	children    []*MemoryQueue
	localClosed bool
}

var (
	memoryMessageCounter uint64
	memoryReceiptCounter uint64
	memorySequence       uint64
)

func NewMemoryQueue() *MemoryQueue {
	return NewMemoryQueueWithConfig(Config{})
}

func NewMemoryQueueWithConfig(config Config) *MemoryQueue {
	config = normalizeConfig(config)
	state := &memoryQueueState{
		topics: make(map[string]*memoryTopic),
		signal: make(chan struct{}, 1),
		config: config,
	}
	queue := &MemoryQueue{state: state}
	go queue.requeueLoop()
	return queue
}

func (m *MemoryQueue) Child(paths ...string) Queue {
	keys := normalizePaths(paths...)
	if len(keys) == 0 {
		return m
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	child := &MemoryQueue{
		state:  m.state,
		prefix: m.prefix + strings.Join(keys, "/") + "/",
		parent: m,
	}
	m.children = append(m.children, child)
	return child
}

func (m *MemoryQueue) Close() error {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.closeLocked()
	m.notifyLocked()
	return nil
}

func (m *MemoryQueue) Publish(ctx context.Context, topic, body string, opts *PublishOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return "", err
	}
	publishOpts, err := m.normalizePublishOptions(opts)
	if err != nil {
		return "", err
	}

	now := time.Now()
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	topicState, err := m.lockTopicState(fullTopic, true)
	if err != nil {
		return "", err
	}
	message := m.publishLocked(topicState, body, publishOpts, now)
	m.notifyLocked()
	return message, nil
}

func (m *MemoryQueue) PublishBatch(ctx context.Context, topic string, messages []PublishRequest) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	normalized := make([]PublishOptions, len(messages))
	for i, request := range messages {
		opts, err := m.normalizePublishOptions(request.Opts)
		if err != nil {
			return nil, err
		}
		normalized[i] = opts
	}

	now := time.Now()
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	topicState, err := m.lockTopicState(fullTopic, true)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(messages))
	for i, request := range messages {
		id := m.publishLocked(topicState, request.Body, normalized[i], now)
		ids = append(ids, id)
	}
	m.notifyLocked()
	return ids, nil
}

func (m *MemoryQueue) Consume(ctx context.Context, topic string, opts *ConsumeOptions) (*Message, error) {
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	consumeOpts, err := normalizeConsumeOptions(m.state.config, opts)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		m.state.mu.Lock()
		if m.isClosedLocked() {
			m.state.mu.Unlock()
			return nil, ErrQueueClosed
		}

		now := time.Now()
		m.reapExpiredLocked(now)
		msg := m.tryConsumeLocked(fullTopic, now, consumeOpts.AckTimeout)
		m.state.mu.Unlock()
		if msg != nil {
			return msg, nil
		}

		wait := m.nextWaitDuration(fullTopic, now)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		case <-m.state.signal:
		}
	}
}

func (m *MemoryQueue) Get(ctx context.Context, topic, messageID string) (*MessageState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return &MessageState{ID: messageID, Status: StatusMissing}, nil
	}
	message := m.messageLocked(state, messageID)
	if message == nil {
		return &MessageState{ID: messageID, Status: StatusMissing}, nil
	}
	stateCopy := stateFromMessage(message)
	return &stateCopy, nil
}

func (m *MemoryQueue) Peek(ctx context.Context, topic string, limit int) ([]MessageState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return []MessageState{}, nil
	}

	peek := make([]MessageState, 0, limit)
	for _, item := range sortedReadyLocked(state) {
		if len(peek) >= limit {
			break
		}
		message := state.messages[item.ID]
		if message == nil || message.Status != StatusReady || message.Sequence != item.Sequence {
			continue
		}
		peek = append(peek, stateFromMessage(message))
	}
	return peek, nil
}

func (m *MemoryQueue) Count(ctx context.Context, topic string) (*Stats, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	stats := &Stats{}
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return stats, nil
	}
	for _, message := range state.messages {
		switch message.Status {
		case StatusReady:
			stats.Ready++
		case StatusInflight:
			stats.Inflight++
		case StatusDead:
			stats.Dead++
		}
	}
	stats.Total = stats.Ready + stats.Inflight + stats.Dead
	return stats, nil
}

func (m *MemoryQueue) GetDead(ctx context.Context, topic, messageID string) (*DeadMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return nil, err
	}
	message, err := m.requireMessageLocked(state, messageID, StatusDead, ErrDeadNotFound)
	if err != nil {
		return nil, err
	}
	dead := deadMessageFromState(message)
	return &dead, nil
}

func (m *MemoryQueue) ListDead(ctx context.Context, topic string, opts *ListOptions) (*DeadListResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return nil, err
	}
	listOpts := normalizeListOptions(opts)

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return &DeadListResult{Messages: []DeadMessage{}}, nil
	}

	dead := sortedDeadLocked(state)
	start := 0
	if listOpts.Cursor != "" {
		index, err := strconv.Atoi(listOpts.Cursor)
		if err == nil && index >= 0 && index <= len(dead) {
			start = index
		}
	}
	end := start + listOpts.Limit
	if end > len(dead) {
		end = len(dead)
	}
	result := &DeadListResult{
		Messages: make([]DeadMessage, 0, end-start),
	}
	for _, message := range dead[start:end] {
		result.Messages = append(result.Messages, deadMessageFromState(message))
	}
	if end < len(dead) {
		result.HasMore = true
		result.NextCursor = strconv.Itoa(end)
	}
	return result, nil
}

func (m *MemoryQueue) CountDead(ctx context.Context, topic string) (int64, error) {
	stats, err := m.Count(ctx, topic)
	if err != nil {
		return 0, err
	}
	return stats.Dead, nil
}

func (m *MemoryQueue) RequeueDead(ctx context.Context, topic, messageID string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 {
		return ErrInvalidDelay
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return err
	}
	message, err := m.requireMessageLocked(state, messageID, StatusDead, ErrDeadNotFound)
	if err != nil {
		return err
	}
	m.markReadyLocked(state, message, time.Now().Add(delay))
	m.notifyLocked()
	return nil
}

func (m *MemoryQueue) RequeueAllDead(ctx context.Context, topic string, limit int, delay time.Duration) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if delay < 0 {
		return 0, ErrInvalidDelay
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return 0, err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return 0, err
	}
	if state == nil {
		return 0, nil
	}

	dead := sortedDeadLocked(state)
	if limit <= 0 || limit > len(dead) {
		limit = len(dead)
	}
	now := time.Now()
	for i := 0; i < limit; i++ {
		m.markReadyLocked(state, dead[i], now.Add(delay))
	}
	if limit > 0 {
		m.notifyLocked()
	}
	return limit, nil
}

func (m *MemoryQueue) DeleteDead(ctx context.Context, topic, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return err
	}
	_, err = m.requireMessageLocked(state, messageID, StatusDead, ErrDeadNotFound)
	if err != nil {
		return err
	}
	m.removeMessageLocked(state, messageID)
	return nil
}

func (m *MemoryQueue) Cancel(ctx context.Context, topic, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return err
	}
	message, err := m.requireMessageLocked(state, messageID, "", ErrMessageNotFound)
	if err != nil {
		return err
	}

	switch message.Status {
	case StatusReady, StatusDead:
		m.markCanceledLocked(state, message)
		m.notifyLocked()
		return nil
	case StatusInflight:
		message.CancelRequested = true
		m.notifyLocked()
		return nil
	case StatusDone, StatusCanceled:
		return ErrInvalidState
	default:
		return ErrInvalidState
	}
}

func (m *MemoryQueue) Delete(ctx context.Context, topic, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fullTopic, err := m.buildTopic(topic)
	if err != nil {
		return err
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.reapExpiredLocked(time.Now())
	state, err := m.lockTopicState(fullTopic, false)
	if err != nil {
		return err
	}
	message, err := m.requireMessageLocked(state, messageID, "", ErrMessageNotFound)
	if err != nil {
		return err
	}
	if message.Status == StatusInflight {
		return ErrInvalidState
	}
	m.removeMessageLocked(state, messageID)
	return nil
}

func (m *MemoryQueue) closeLocked() {
	if m.localClosed {
		return
	}
	m.localClosed = true
	for _, child := range m.children {
		child.closeLocked()
	}
}

func (m *MemoryQueue) isClosedLocked() bool {
	for current := m; current != nil; current = current.parent {
		if current.localClosed {
			return true
		}
	}
	return m.state.closed
}

func (m *MemoryQueue) buildTopic(topic string) (string, error) {
	topic = strings.Trim(topic, "/")
	if topic == "" {
		return "", ErrInvalidTopic
	}
	if m.prefix == "" {
		return topic, nil
	}
	return m.prefix + topic, nil
}

func (m *MemoryQueue) normalizePublishOptions(opts *PublishOptions) (PublishOptions, error) {
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
		normalized.MaxDeliveries = m.state.config.MaxDeliveries
	}
	return normalized, nil
}

func (m *MemoryQueue) requeueLoop() {
	ticker := time.NewTicker(m.state.config.RequeueInterval)
	defer ticker.Stop()

	for range ticker.C {
		m.state.mu.Lock()
		if m.localClosed {
			m.state.closed = true
			m.state.mu.Unlock()
			return
		}
		if m.state.closed {
			m.state.mu.Unlock()
			return
		}
		before := countActiveLocked(m.state.topics)
		m.reapExpiredLocked(time.Now())
		after := countActiveLocked(m.state.topics)
		if after != before {
			m.notifyLocked()
		}
		m.state.mu.Unlock()
	}
}

func (m *MemoryQueue) reapExpiredLocked(now time.Time) {
	for topicName, topic := range m.state.topics {
		for _, message := range topic.messages {
			if message.Status != StatusInflight || now.Before(message.Deadline) {
				continue
			}
			m.requeueLocked(topic, message, message.VisibleAt, now)
		}
		if len(topic.messages) == 0 {
			delete(m.state.topics, topicName)
		}
	}
}

func (m *MemoryQueue) requeueLocked(topic *memoryTopic, message *memoryMessage, visibleAt, now time.Time) {
	if message.MaxDeliveries > 0 && message.Attempt >= message.MaxDeliveries {
		message.Status = StatusDead
		message.DeadAt = now
		message.Deadline = time.Time{}
		message.VisibleAt = time.Time{}
		message.Receipt = ""
		return
	}
	if visibleAt.Before(now) {
		visibleAt = now
	}
	m.markReadyLocked(topic, message, visibleAt)
}

func (m *MemoryQueue) enqueueLocked(topic *memoryTopic, message *memoryMessage) {
	message.Status = StatusReady
	message.Sequence = nextSequence()
	topic.ready = append(topic.ready, &memoryReady{
		ID:        message.ID,
		VisibleAt: message.VisibleAt,
		Sequence:  message.Sequence,
	})
	sort.Slice(topic.ready, func(i, j int) bool {
		if !topic.ready[i].VisibleAt.Equal(topic.ready[j].VisibleAt) {
			return topic.ready[i].VisibleAt.Before(topic.ready[j].VisibleAt)
		}
		return topic.ready[i].Sequence < topic.ready[j].Sequence
	})
}

func (m *MemoryQueue) tryConsumeLocked(topicName string, now time.Time, ackTimeout time.Duration) *Message {
	topic := m.state.topics[topicName]
	if topic == nil {
		return nil
	}

	for len(topic.ready) > 0 {
		item := topic.ready[0]
		message := topic.messages[item.ID]
		if message == nil || message.Status != StatusReady || message.Sequence != item.Sequence {
			topic.ready = topic.ready[1:]
			continue
		}
		if item.VisibleAt.After(now) {
			return nil
		}

		topic.ready = topic.ready[1:]
		message.Status = StatusInflight
		message.Attempt++
		message.Receipt = nextReceiptID()
		message.Deadline = now.Add(ackTimeout)
		message.CancelRequested = false
		receipt := message.Receipt

		return newClaimedMessage(
			message.ID,
			message.Body,
			message.Attempt,
			receipt,
			message.PublishedAt,
			message.VisibleAt,
			message.Deadline,
			func(ctx context.Context) error {
				return m.ack(ctx, topicName, message.ID, receipt)
			},
			func(ctx context.Context, delay time.Duration) error {
				return m.nack(ctx, topicName, message.ID, receipt, delay)
			},
			func(ctx context.Context, ttl time.Duration) error {
				return m.touch(ctx, topicName, message.ID, receipt, ttl)
			},
			func(ctx context.Context) (bool, error) {
				return m.cancelRequested(ctx, topicName, message.ID, receipt)
			},
		)
	}
	return nil
}

func (m *MemoryQueue) ack(ctx context.Context, topicName, messageID, receipt string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.isClosedLocked() {
		return ErrQueueClosed
	}
	topic, _ := m.lockTopicState(topicName, false)
	message, err := m.requireInflightLocked(topic, messageID, receipt)
	if err != nil {
		return err
	}
	m.markDoneLocked(topic, message)
	return nil
}

func (m *MemoryQueue) nack(ctx context.Context, topicName, messageID, receipt string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 {
		return ErrInvalidDelay
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.isClosedLocked() {
		return ErrQueueClosed
	}
	topic, _ := m.lockTopicState(topicName, false)
	message, err := m.requireInflightLocked(topic, messageID, receipt)
	if err != nil {
		return err
	}
	m.requeueLocked(topic, message, time.Now().Add(delay), time.Now())
	m.notifyLocked()
	return nil
}

func (m *MemoryQueue) touch(ctx context.Context, topicName, messageID, receipt string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrInvalidAckTimeout
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.isClosedLocked() {
		return ErrQueueClosed
	}
	topic, _ := m.lockTopicState(topicName, false)
	message, err := m.requireInflightLocked(topic, messageID, receipt)
	if err != nil {
		return err
	}
	message.Deadline = time.Now().Add(ttl)
	m.notifyLocked()
	return nil
}

func (m *MemoryQueue) cancelRequested(ctx context.Context, topicName, messageID, receipt string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	if m.isClosedLocked() {
		return false, ErrQueueClosed
	}
	topic, _ := m.lockTopicState(topicName, false)
	message, err := m.requireInflightLocked(topic, messageID, receipt)
	if err != nil {
		return false, err
	}
	return message.CancelRequested, nil
}

func (m *MemoryQueue) requireInflightLocked(topic *memoryTopic, messageID, receipt string) (*memoryMessage, error) {
	if topic == nil {
		return nil, ErrReceiptMismatch
	}
	message := topic.messages[messageID]
	if message == nil || message.Status != StatusInflight || message.Receipt != receipt {
		return nil, ErrReceiptMismatch
	}
	return message, nil
}

func (m *MemoryQueue) nextWaitDuration(topic string, now time.Time) time.Duration {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()

	wait := m.state.config.RequeueInterval
	state := m.state.topics[topic]
	if state == nil {
		return wait
	}

	for _, item := range state.ready {
		message := state.messages[item.ID]
		if message == nil || message.Status != StatusReady || message.Sequence != item.Sequence {
			continue
		}
		if !item.VisibleAt.After(now) {
			return time.Millisecond
		}
		if candidate := item.VisibleAt.Sub(now); candidate < wait {
			wait = candidate
		}
	}
	for _, message := range state.messages {
		if message.Status != StatusInflight {
			continue
		}
		if !message.Deadline.After(now) {
			return time.Millisecond
		}
		if candidate := message.Deadline.Sub(now); candidate < wait {
			wait = candidate
		}
	}
	if wait <= 0 {
		return time.Millisecond
	}
	return wait
}

func (m *MemoryQueue) topicLocked(topic string) *memoryTopic {
	state := m.state.topics[topic]
	if state != nil {
		return state
	}
	state = &memoryTopic{
		messages: make(map[string]*memoryMessage),
		dedup:    make(map[string]string),
	}
	m.state.topics[topic] = state
	return state
}

func (m *MemoryQueue) lockTopicState(topic string, create bool) (*memoryTopic, error) {
	if m.isClosedLocked() {
		return nil, ErrQueueClosed
	}
	if create {
		return m.topicLocked(topic), nil
	}
	return m.state.topics[topic], nil
}

func (m *MemoryQueue) messageLocked(topic *memoryTopic, messageID string) *memoryMessage {
	if topic == nil {
		return nil
	}
	return topic.messages[messageID]
}

func (m *MemoryQueue) requireMessageLocked(topic *memoryTopic, messageID string, status MessageStatus, errNotFound error) (*memoryMessage, error) {
	message := m.messageLocked(topic, messageID)
	if message == nil {
		return nil, errNotFound
	}
	if status != "" && message.Status != status {
		return nil, errNotFound
	}
	return message, nil
}

func (m *MemoryQueue) publishLocked(topic *memoryTopic, body string, opts PublishOptions, now time.Time) string {
	if opts.DedupKey != "" {
		if existingID, ok := topic.dedup[opts.DedupKey]; ok {
			if existing := topic.messages[existingID]; existing != nil && existing.Status != StatusDone && existing.Status != StatusCanceled {
				return existing.ID
			}
			delete(topic.dedup, opts.DedupKey)
		}
	}

	message := &memoryMessage{
		ID:            nextMessageID(),
		Body:          body,
		DedupKey:      opts.DedupKey,
		MaxDeliveries: opts.MaxDeliveries,
		Status:        StatusReady,
		PublishedAt:   now,
		VisibleAt:     now.Add(opts.Delay),
	}
	topic.messages[message.ID] = message
	if message.DedupKey != "" {
		topic.dedup[message.DedupKey] = message.ID
	}
	m.enqueueLocked(topic, message)
	return message.ID
}

func (m *MemoryQueue) markReadyLocked(topic *memoryTopic, message *memoryMessage, visibleAt time.Time) {
	message.DeadAt = time.Time{}
	message.CancelRequested = false
	message.Receipt = ""
	message.Deadline = time.Time{}
	message.VisibleAt = visibleAt
	m.enqueueLocked(topic, message)
}

func (m *MemoryQueue) markDoneLocked(topic *memoryTopic, message *memoryMessage) {
	m.releaseDedupLocked(topic, message)
	message.Status = StatusDone
	message.VisibleAt = time.Time{}
	message.Deadline = time.Time{}
	message.Receipt = ""
	message.CancelRequested = false
}

func (m *MemoryQueue) markCanceledLocked(topic *memoryTopic, message *memoryMessage) {
	m.releaseDedupLocked(topic, message)
	message.Status = StatusCanceled
	message.VisibleAt = time.Time{}
	message.Deadline = time.Time{}
	message.DeadAt = time.Time{}
	message.Receipt = ""
	message.CancelRequested = true
}

func (m *MemoryQueue) removeMessageLocked(topic *memoryTopic, messageID string) {
	message := topic.messages[messageID]
	if message == nil {
		return
	}
	m.releaseDedupLocked(topic, message)
	delete(topic.messages, messageID)
}

func (m *MemoryQueue) releaseDedupLocked(topic *memoryTopic, message *memoryMessage) {
	if message.DedupKey == "" {
		return
	}
	if topic.dedup[message.DedupKey] == message.ID {
		delete(topic.dedup, message.DedupKey)
	}
}

func (m *MemoryQueue) notifyLocked() {
	select {
	case m.state.signal <- struct{}{}:
	default:
	}
}

func stateFromMessage(message *memoryMessage) MessageState {
	return MessageState{
		ID:              message.ID,
		Body:            message.Body,
		Status:          message.Status,
		Attempt:         message.Attempt,
		PublishedAt:     message.PublishedAt,
		VisibleAt:       message.VisibleAt,
		Deadline:        message.Deadline,
		Receipt:         message.Receipt,
		CancelRequested: message.CancelRequested,
		DeadAt:          message.DeadAt,
	}
}

func deadMessageFromState(message *memoryMessage) DeadMessage {
	return DeadMessage{
		ID:              message.ID,
		Body:            message.Body,
		Attempt:         message.Attempt,
		PublishedAt:     message.PublishedAt,
		DeadAt:          message.DeadAt,
		CancelRequested: message.CancelRequested,
	}
}

func sortedReadyLocked(topic *memoryTopic) []*memoryReady {
	ready := make([]*memoryReady, 0, len(topic.ready))
	ready = append(ready, topic.ready...)
	sort.Slice(ready, func(i, j int) bool {
		if !ready[i].VisibleAt.Equal(ready[j].VisibleAt) {
			return ready[i].VisibleAt.Before(ready[j].VisibleAt)
		}
		return ready[i].Sequence < ready[j].Sequence
	})
	return ready
}

func sortedDeadLocked(topic *memoryTopic) []*memoryMessage {
	dead := make([]*memoryMessage, 0)
	for _, message := range topic.messages {
		if message.Status == StatusDead {
			dead = append(dead, message)
		}
	}
	sort.Slice(dead, func(i, j int) bool {
		if !dead[i].DeadAt.Equal(dead[j].DeadAt) {
			return dead[i].DeadAt.Before(dead[j].DeadAt)
		}
		if !dead[i].PublishedAt.Equal(dead[j].PublishedAt) {
			return dead[i].PublishedAt.Before(dead[j].PublishedAt)
		}
		return dead[i].ID < dead[j].ID
	})
	return dead
}

func countActiveLocked(topics map[string]*memoryTopic) int {
	total := 0
	for _, topic := range topics {
		for _, message := range topic.messages {
			switch message.Status {
			case StatusReady, StatusInflight, StatusDead:
				total++
			}
		}
	}
	return total
}

func nextMessageID() string {
	return fmt.Sprintf("msg-%d", atomic.AddUint64(&memoryMessageCounter, 1))
}

func nextReceiptID() string {
	return fmt.Sprintf("rcpt-%d", atomic.AddUint64(&memoryReceiptCounter, 1))
}

func nextSequence() uint64 {
	return atomic.AddUint64(&memorySequence, 1)
}

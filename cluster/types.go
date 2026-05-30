package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	defaultWatchBufferSize     = 256
	defaultEventRetentionCount = 2000
	defaultEventBatchSize      = 512
	defaultListLimit           = 100
	maxMutationRetries         = 8
)

var (
	ErrClosed                = errors.New("cluster: closed")
	ErrInvalidConfig         = errors.New("cluster: invalid config")
	ErrInvalidResource       = errors.New("cluster: invalid resource")
	ErrInvalidObject         = errors.New("cluster: invalid object")
	ErrAlreadyExists         = errors.New("cluster: object already exists")
	ErrNotFound              = errors.New("cluster: object not found")
	ErrConflict              = errors.New("cluster: resource version conflict")
	ErrResourceVersionTooOld = errors.New("cluster: resource version too old")
	ErrUnsupported           = errors.New("cluster: unsupported operation")
)

type Options struct {
	// Prefix scopes persistent keys for badger and etcd backends.
	Prefix string
	// EventRetentionCount keeps only the newest N events. Zero uses the default.
	EventRetentionCount int
	// WatchBufferSize controls each Watch result channel buffer.
	WatchBufferSize int
}

type Labels map[string]string

type Annotations map[string]string

type Object[S, T any] struct {
	APIVersion string   `json:"apiVersion"`
	Kind       string   `json:"kind"`
	Metadata   Metadata `json:"metadata"`
	Spec       S        `json:"spec,omitempty"`
	Status     T        `json:"status,omitempty"`
}

type Metadata struct {
	Name            string      `json:"name"`
	UID             string      `json:"uid"`
	ResourceVersion string      `json:"resourceVersion"`
	Generation      int64       `json:"generation"`
	CreatedAt       time.Time   `json:"createdAt"`
	UpdatedAt       time.Time   `json:"updatedAt"`
	DeletedAt       *time.Time  `json:"deletedAt,omitempty"`
	Labels          Labels      `json:"labels,omitempty"`
	Annotations     Annotations `json:"annotations,omitempty"`
	Finalizers      []string    `json:"finalizers,omitempty"`
}

type ObjectList[S, T any] struct {
	Items           []Object[S, T] `json:"items"`
	ResourceVersion string         `json:"resourceVersion"`
	Continue        string         `json:"continue,omitempty"`
}

type Unstructured struct {
	APIVersion string          `json:"apiVersion"`
	Kind       string          `json:"kind"`
	Metadata   Metadata        `json:"metadata"`
	Spec       json.RawMessage `json:"spec,omitempty"`
	Status     json.RawMessage `json:"status,omitempty"`
}

type UnstructuredList struct {
	Items           []Unstructured `json:"items"`
	ResourceVersion string         `json:"resourceVersion"`
	Continue        string         `json:"continue,omitempty"`
}

type objectRef struct {
	Resource string `json:"resource"`
	Name     string `json:"name"`
}

type Subresource string

const (
	SubresourceSpec   Subresource = "spec"
	SubresourceStatus Subresource = "status"
)

type CreateOptions struct {
	Labels           Labels
	Annotations      Annotations
	Finalizers       []string
	EventAnnotations Annotations
}

type UpdateOptions struct {
	ResourceVersion  string
	EventAnnotations Annotations
}

type PatchOptions struct {
	ResourceVersion  string
	EventAnnotations Annotations
}

type DeleteOptions struct {
	ResourceVersion  string
	EventAnnotations Annotations
}

type ListOptions struct {
	Limit    int
	Continue string
	Selector Selector
}

type WatchOptions struct {
	Name              string
	Since             string
	Selector          Selector
	AllowBookmarks    bool
	SendInitialEvents bool
}

type WatchEventType string

const (
	WatchAdded    WatchEventType = "ADDED"
	WatchModified WatchEventType = "MODIFIED"
	WatchDeleted  WatchEventType = "DELETED"
	WatchBookmark WatchEventType = "BOOKMARK"
	WatchError    WatchEventType = "ERROR"
)

type WatchEvent[S, T any] struct {
	Type            WatchEventType `json:"type"`
	ResourceVersion string         `json:"resourceVersion"`
	Object          *Object[S, T]  `json:"object,omitempty"`
	Annotations     Annotations    `json:"annotations,omitempty"`
	Changed         []string       `json:"changed,omitempty"`
	Error           error          `json:"-"`
}

type UnstructuredWatchEvent struct {
	Type            WatchEventType `json:"type"`
	ResourceVersion string         `json:"resourceVersion"`
	Object          *Unstructured  `json:"object,omitempty"`
	Annotations     Annotations    `json:"annotations,omitempty"`
	Changed         []string       `json:"changed,omitempty"`
	Error           error          `json:"-"`
}

type resourceEvent struct {
	Type            WatchEventType `json:"type"`
	ResourceVersion string         `json:"resourceVersion"`
	Ref             objectRef      `json:"ref"`
	Object          *Unstructured  `json:"object,omitempty"`
	Annotations     Annotations    `json:"annotations,omitempty"`
	Changed         []string       `json:"changed,omitempty"`
}

type resourceStore interface {
	get(context.Context, objectRef) (*Unstructured, error)
	list(context.Context, string) ([]Unstructured, uint64, error)
	commit(context.Context, commitRequest) (*Unstructured, resourceEvent, error)
	eventsAfter(context.Context, uint64, string, int) ([]resourceEvent, uint64, error)
	compact(context.Context, uint64) error
	subscribe(context.Context, string) (<-chan struct{}, func(), error)
	close() error
}

type commitOp int

const (
	commitCreate commitOp = iota
	commitUpdate
	commitDelete
)

type commitRequest struct {
	Op               commitOp
	Ref              objectRef
	ExpectedRV       uint64
	Object           *Unstructured
	EventType        WatchEventType
	EventAnnotations Annotations
	Changed          []string
}

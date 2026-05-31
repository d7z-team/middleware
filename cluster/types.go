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
	minBackgroundInterval      = 10 * time.Millisecond
	defaultNodeLeaseTTL        = 30 * time.Second
	defaultNodeRenewInterval   = 10 * time.Second
	defaultMasterHistoryLimit  = 2000

	ResourceNodes   = "nodes"
	ResourceMasters = "masters"
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
	ErrNodeAlreadyExists     = errors.New("cluster: node already exists")
	ErrNodeLeaseLost         = errors.New("cluster: node lease lost")
	ErrNotMaster             = errors.New("cluster: not master")
)

type Options struct {
	// Prefix scopes persistent keys for badger and etcd backends.
	Prefix string
	// NodeName identifies this cluster client instance and is required.
	NodeName string
	// NodeLeaseTTL controls how long another process must wait after a crash before reusing this node name.
	NodeLeaseTTL time.Duration
	// NodeRenewInterval controls how often the node lease is renewed. Values below 10ms are rejected.
	NodeRenewInterval time.Duration
	// MasterLeaseTTL controls how long a master term stays valid without renewal. Zero uses NodeLeaseTTL.
	MasterLeaseTTL time.Duration
	// MasterRenewInterval controls how often the current master term is renewed. Zero uses NodeRenewInterval. Values below 10ms are rejected.
	MasterRenewInterval time.Duration
	// MasterHistoryLimit keeps only the newest N master transition records. Zero uses the default.
	MasterHistoryLimit int
	// EventRetentionCount keeps only the newest N events. Zero uses the default.
	EventRetentionCount int
	// EventCleanupInterval controls how often the master cleans old watch events. Zero uses MasterRenewInterval. Values below 10ms are rejected.
	EventCleanupInterval time.Duration
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
	Namespace       string      `json:"namespace,omitempty"`
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

type NodeSpec struct {
	Metadata Annotations `json:"metadata,omitempty"`
}

type NodeStatus struct {
	Metadata   Annotations `json:"metadata,omitempty"`
	LeaseUntil time.Time   `json:"leaseUntil,omitempty"`
	UpdatedAt  time.Time   `json:"updatedAt,omitempty"`
}

type MasterSpec struct{}

type MasterStatus struct {
	Node       string             `json:"node,omitempty"`
	Term       uint64             `json:"term,omitempty"`
	LeaseUntil time.Time          `json:"leaseUntil,omitempty"`
	AcquiredAt time.Time          `json:"acquiredAt,omitempty"`
	RenewedAt  time.Time          `json:"renewedAt,omitempty"`
	History    []MasterTransition `json:"history,omitempty"`
}

type MasterTransition struct {
	Term               uint64    `json:"term"`
	From               string    `json:"from,omitempty"`
	To                 string    `json:"to,omitempty"`
	Reason             string    `json:"reason"`
	At                 time.Time `json:"at"`
	ObservedBy         string    `json:"observedBy"`
	PreviousLeaseUntil time.Time `json:"previousLeaseUntil,omitempty"`
}

type MasterInfo struct {
	Node            string    `json:"node,omitempty"`
	Term            uint64    `json:"term,omitempty"`
	LeaseUntil      time.Time `json:"leaseUntil,omitempty"`
	AcquiredAt      time.Time `json:"acquiredAt,omitempty"`
	RenewedAt       time.Time `json:"renewedAt,omitempty"`
	ResourceVersion string    `json:"resourceVersion,omitempty"`
	Valid           bool      `json:"valid"`
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
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

type resourceScope struct {
	Resource      string
	Namespace     string
	AllNamespaces bool
}

type Subresource string

const (
	SubresourceSpec     Subresource = "spec"
	SubresourceMetadata Subresource = "metadata"
	SubresourceStatus   Subresource = "status"
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
	Scope             WatchScope
	AllowBookmarks    bool
	SendInitialEvents bool
}

type WatchScope string

const (
	WatchScopeObject   WatchScope = "object"
	WatchScopeMetadata WatchScope = "metadata"
	WatchScopeStatus   WatchScope = "status"
)

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

type MasterWatchEvent struct {
	Type            WatchEventType    `json:"type"`
	ResourceVersion string            `json:"resourceVersion"`
	Master          *MasterInfo       `json:"master,omitempty"`
	Transition      *MasterTransition `json:"transition,omitempty"`
	Error           error             `json:"-"`
}

type ResourceInfo struct {
	Resource    string
	APIVersion  string
	Kind        string
	Namespaced  bool
	Spec        []FieldInfo
	Status      []FieldInfo
	Annotations []AnnotationRule
	Builtin     bool
}

type FieldInfo struct {
	Path      string
	Required  bool
	Immutable bool
	Indexed   bool
	IndexName string
	Enum      []string
}

type resourceEvent struct {
	Type            WatchEventType `json:"type"`
	ResourceVersion string         `json:"resourceVersion"`
	Ref             objectRef      `json:"ref"`
	Object          *Unstructured  `json:"object,omitempty"`
	Annotations     Annotations    `json:"annotations,omitempty"`
	Changed         []string       `json:"changed,omitempty"`
}

type nodeLeaseRecord struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type resourceStore interface {
	get(context.Context, objectRef) (*Unstructured, error)
	list(context.Context, resourceScope) ([]Unstructured, uint64, error)
	commit(context.Context, commitRequest) (*Unstructured, resourceEvent, error)
	eventsAfter(context.Context, uint64, resourceScope, int) ([]resourceEvent, uint64, error)
	cleanupEvents(context.Context) error
	subscribe(context.Context, resourceScope) (<-chan struct{}, func(), error)
	acquireNode(context.Context, string, time.Duration) (string, error)
	renewNode(context.Context, string, string, time.Duration) error
	releaseNode(context.Context, string, string) error
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

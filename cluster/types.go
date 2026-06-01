package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	defaultWatchBufferSize            = 256
	defaultEventRetentionCount        = 2000
	defaultEventBatchSize             = 512
	defaultListLimit                  = 100
	maxMutationRetries                = 8
	minBackgroundInterval             = 10 * time.Millisecond
	defaultNodeLeaseTTL               = 30 * time.Second
	defaultNodeRenewInterval          = 10 * time.Second
	defaultMasterHistoryLimit         = 2000
	defaultAdmissionTimeout           = 30 * time.Second
	defaultAdmissionRetention         = 2000
	defaultAdmissionTerminalRetention = 10 * time.Minute

	ResourceNodes             = "nodes"
	ResourceMasters           = "masters"
	ResourceAdmissionRequests = "admissionrequests"
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
	ErrAdmissionPending      = errors.New("cluster: admission pending")
	ErrAdmissionRejected     = errors.New("cluster: admission rejected")
	ErrAdmissionExpired      = errors.New("cluster: admission expired")
	ErrAdmissionCanceled     = errors.New("cluster: admission canceled")
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
	// AdmissionTimeout controls synchronous write wait time when a resource requires admission. Zero uses 30s.
	AdmissionTimeout time.Duration
	// AdmissionRetentionCount keeps only the newest N terminal admission requests. Zero uses the default.
	AdmissionRetentionCount int
	// AdmissionTerminalRetention keeps terminal admission requests for at least this long. Zero uses the default.
	AdmissionTerminalRetention time.Duration
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
	Resource   string
	APIVersion string
	Kind       string
	Namespaced bool
	Schema     json.RawMessage
	Indexes    []IndexInfo
	Admission  []AdmissionRule
	Builtin    bool
}

type IndexInfo struct {
	Path string
}

type AdmissionOperation string

const (
	AdmissionCreate AdmissionOperation = "CREATE"
	AdmissionUpdate AdmissionOperation = "UPDATE"
	AdmissionDelete AdmissionOperation = "DELETE"
)

type AdmissionRule struct {
	Name         string
	Operations   []AdmissionOperation
	Subresources []Subresource
	Timeout      time.Duration
}

type AdmissionRequestSpec struct {
	Rules       []string           `json:"rules,omitempty"`
	Operation   AdmissionOperation `json:"operation,omitempty"`
	Resource    string             `json:"resource,omitempty"`
	APIVersion  string             `json:"apiVersion,omitempty"`
	Kind        string             `json:"kind,omitempty"`
	Namespaced  bool               `json:"namespaced,omitempty"`
	Namespace   string             `json:"namespace,omitempty"`
	Name        string             `json:"name,omitempty"`
	Subresource Subresource        `json:"subresource,omitempty"`

	Precondition      AdmissionPrecondition `json:"precondition,omitempty"`
	SchemaFingerprint string                `json:"schemaFingerprint,omitempty"`
	OldObject         *Unstructured         `json:"oldObject,omitempty"`
	Object            *Unstructured         `json:"object,omitempty"`
	EventAnnotations  Annotations           `json:"eventAnnotations,omitempty"`

	CreatedByNode string    `json:"createdByNode,omitempty"`
	ExpiresAt     time.Time `json:"expiresAt,omitempty"`
}

type AdmissionPrecondition struct {
	MustExist       bool   `json:"mustExist,omitempty"`
	MustNotExist    bool   `json:"mustNotExist,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type AdmissionPhase string

const (
	AdmissionPendingPhase   AdmissionPhase = "Pending"
	AdmissionRejectedPhase  AdmissionPhase = "Rejected"
	AdmissionExpiredPhase   AdmissionPhase = "Expired"
	AdmissionCanceledPhase  AdmissionPhase = "Canceled"
	AdmissionCommittedPhase AdmissionPhase = "Committed"
)

type AdmissionRuleDecision struct {
	Rule    string    `json:"rule,omitempty"`
	Message string    `json:"message,omitempty"`
	Decider string    `json:"decider,omitempty"`
	At      time.Time `json:"at,omitempty"`
}

type AdmissionRequestStatus struct {
	Phase                 AdmissionPhase          `json:"phase,omitempty"`
	Approved              []AdmissionRuleDecision `json:"approved,omitempty"`
	RejectedRule          string                  `json:"rejectedRule,omitempty"`
	Message               string                  `json:"message,omitempty"`
	DecidedBy             string                  `json:"decidedBy,omitempty"`
	DecidedAt             time.Time               `json:"decidedAt,omitempty"`
	LastError             string                  `json:"lastError,omitempty"`
	LastErrorAt           time.Time               `json:"lastErrorAt,omitempty"`
	TargetResourceVersion string                  `json:"targetResourceVersion,omitempty"`
	TargetObject          *Unstructured           `json:"targetObject,omitempty"`
}

type AdmissionDecisionOptions struct {
	Rule    string
	Message string
	Decider string
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
	admissionPending(context.Context, objectRef) (string, error)
	beginAdmission(context.Context, beginAdmissionRequest) (*Unstructured, error)
	approveAdmission(context.Context, approveAdmissionRequest) (*Unstructured, *Unstructured, error)
	rejectAdmission(context.Context, rejectAdmissionRequest) (*Unstructured, error)
	expireAdmission(context.Context, string, AdmissionPhase, string) (*Unstructured, error)
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
	Op                commitOp
	Ref               objectRef
	ExpectedRV        uint64
	SkipAdmissionLock bool
	Object            *Unstructured
	EventType         WatchEventType
	EventAnnotations  Annotations
	Changed           []string
}

type beginAdmissionRequest struct {
	Request *Unstructured
	Target  objectRef
}

type approveAdmissionRequest struct {
	Name           string
	Decision       AdmissionDecisionOptions
	RequireRule    string
	RequirePending bool
}

type rejectAdmissionRequest struct {
	Name     string
	Decision AdmissionDecisionOptions
}

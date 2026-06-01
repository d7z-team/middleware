# cluster 最终设计待办

本文档定义 cluster 的最终实现目标：将其收敛成一个内嵌式 Kubernetes-style API store。运行时以 JSON Schema 为唯一资源约束，Go SDK 和底层 store 语义保持清晰一致。范围聚焦资源定义、结构校验、CRUD/watch、status 子资源、field selector、admission 内部资源和三种后端的一致行为。

参考语义以 Kubernetes 为准：

- API Concepts: https://kubernetes.io/docs/reference/using-api/api-concepts/
- CRD: https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
- Admission Control: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/

## 1. 总体设计

- cluster 是内嵌式 API store。
- Go SDK 是对外操作面。
- 资源模型采用单版本资源。
- 存储后端仍然只有 `memory`、`badger`、`etcd`。
- `memory` 和 `badger` 是单实例语义；`etcd` 支持多实例与 master 选举。
- Patch 采用 JSON merge patch。
- 所有未明确的对象生命周期、resourceVersion、watch、status 子资源语义参考 Kubernetes。

## 2. JSON Schema 资源定义

运行时只认 JSON Schema。泛型只用于生成 schema 和 typed wrapper。

```go
type ResourceDef struct {
	Resource   string
	APIVersion string
	Kind       string
	Namespaced bool

	Schema    json.RawMessage
	Admission []AdmissionRule
}

type TypedResourceDef[S, T any] struct {
	Resource   string
	APIVersion string
	Kind       string
	Namespaced bool

	Schema    json.RawMessage
	Admission []AdmissionRule
}

func DefineResource(c *Cluster, def ResourceDef) (*UnstructuredResource, error)
func Define[S, T any](c *Cluster, def TypedResourceDef[S, T]) (*Resource[S, T], error)
func SchemaFrom[S, T any](apiVersion, kind string, namespaced bool) (json.RawMessage, error)
```

Schema 规则：

- 使用 JSON Schema Draft 2020-12。
- root 必须是 object。
- root 固定字段为 `apiVersion`、`kind`、`metadata`、`spec`、`status`。
- `spec` 必须声明 schema。
- `status` 可选；未声明时 status 只能为空对象。
- 默认裁剪未知字段，对齐 Kubernetes CRD pruning。
- 需要保留自由 JSON 的节点必须显式声明 `x-cluster-preserve-unknown-fields: true`。
- `$ref` 的解析范围限定为当前 schema 内的 `$defs`。
- schema 注册时校验 schema 自身、默认值、扩展字段位置，失败则拒绝 `Define`。
- schema 必须满足 Kubernetes structural schema 思路：每个对象节点必须有明确 `type`，数组必须声明 `items`，map 必须通过 `additionalProperties` 明确 value schema，不能依赖隐式自由结构。
- `oneOf`、`anyOf`、`allOf`、`not` 作为附加校验使用；root 结构、托管 metadata 字段和字段形态由 structural schema 主体定义。
- `default` 注册时按当前节点的 resolved structural schema 先裁剪，再校验；保存裁剪后的 default。

保留的 cluster 扩展：

```json
{
  "x-cluster-immutable": true,
  "x-cluster-index": true,
  "x-cluster-index-keys": ["tenant"],
  "x-cluster-preserve-unknown-fields": true
}
```

- `x-cluster-immutable`：适用于 `spec` 或可写 metadata 字段，表达创建后保持稳定的字段。
- `x-cluster-index`：声明字段可用于 `Field(...)` 查询；字段必须是稳定 scalar。
- `x-cluster-index-keys`：仅允许用于 `metadata.labels` 和 `metadata.annotations` 的 string map schema 节点，声明允许 selector 查询的 key。
- `x-cluster-preserve-unknown-fields`：关闭当前节点的未知字段裁剪。

定义职责：

- JSON Schema 表达结构、默认值、枚举、格式、未知字段裁剪、索引和不可变约束。
- Admission 表达需要外部检查者确认的动态写入约束。
- Typed API 负责类型包装和 schema 生成；运行时以编译后的 JSON Schema 和 admission 声明为准。

## 3. 资源创建检查与结构演进

资源定义创建后进入冻结状态。后续重新 `Define` 同一个 `resource` / `apiVersion` / `kind` 时，定义指纹必须完全相同。

创建检查：

- `Resource`、`APIVersion`、`Kind`、`Namespaced` 和 `Admission` 是资源身份的一部分，注册后保持稳定。
- `spec`、`status` 的顶层结构保持稳定。
- 已存在字段保持 JSON 类型、nullable、required、enum、format、pattern、数值范围、字符串长度、数组长度和 `x-cluster-immutable` 语义。
- 已存在对象字段保持未知字段裁剪策略。
- 已存在索引保持可用；新增索引在注册成功后立即对已有对象生效。
- 已存在字段的 default 可以补充，作用范围限定为后续写入和新建对象。
- 结构演进以新增可选 `spec` 字段、新增 `status` 字段和新增索引为主。
- 字段删除、字段类型变化、required 收紧、约束收紧、nullable 收紧、索引移除和 admission 声明变化使用新的 `resource` 或新的 `apiVersion/kind` 表达。

实现策略：

- 同名资源重复 `Define` 只允许 schema、admission 声明和基础身份完全相同，返回已有定义或明确拒绝。
- schema 指纹必须基于 canonical JSON 生成，忽略对象 key 顺序，不忽略语义字段差异。
- definition 指纹必须覆盖基础身份、namespaced、schema 指纹和 admission 声明。
- 结构性版本切换使用新的 `resource` 或新的 `apiVersion/kind`。

这部分遵循 Kubernetes CRD 的基本取舍：资源结构创建后不能随意破坏，schema 需要保持结构化，演进以新增可选字段为主。

## 4. Metadata 与子资源

metadata 语义参考 Kubernetes：

- `metadata.name` 创建时指定，创建后不可变。
- namespaced 资源的 `metadata.namespace` 由 `Namespace("x")` 句柄决定，创建后不可变。
- cluster-scoped 资源 namespace 必须为空。
- `uid`、`resourceVersion`、`generation`、`createdAt`、`updatedAt`、`deletedAt` 全部由 cluster 管理。
- `generation` 只在 `spec` 变化时递增。
- 主资源 `Update` / `Patch` 可同时修改 `spec` 和 metadata 可写字段。
- metadata 可写字段为 `labels`、`annotations`、`finalizers`。
- `PatchMetadata` 是 SDK 便利方法，与主资源 metadata 写入共用同一套校验和 admission 规则，只修改 metadata 可写字段。
- `status` 必须通过 `UpdateStatus` / `PatchStatus` 修改。
- `Create` 的 status 必须为空。
- `Update` / `Patch` 携带 status 时必须与旧对象 status 完全一致。
- 删除有 finalizer 的对象时只设置 `deletedAt` 并产生 `MODIFIED`；finalizer 清空后再次删除才真正移除并产生 `DELETED`。

## 5. CRUD、Patch、Watch

CRUD 规则：

- `Create` 校验最终完整对象。
- `Update` 是完整对象更新，但禁止通过主资源更新 status。
- `Patch` 使用 JSON merge patch，校验 patch 后的最终对象。
- `Create` 的 status 必须为空；`Update`、`Patch` 的 status 必须与旧对象完全一致。
- `PatchMetadata` 只修改 metadata 可写字段，不递增 generation。
- `UpdateStatus` / `PatchStatus` 只修改 status，不递增 generation。
- `ResourceVersion` 作为 CAS 条件；传入则必须精确匹配，不传则内部有限重试。
- 所有写入都先恢复托管字段，再应用默认值、裁剪未知字段、schema 校验、不可变校验，最后提交。

List / Watch 规则：

- `List` 返回集合的 `resourceVersion` 和 `continue`。
- `Watch` 使用 `Since` 从指定版本后开始。
- 太旧的 `Since` 返回 `ErrResourceVersionTooOld`。
- 支持 `ADDED`、`MODIFIED`、`DELETED`、`BOOKMARK`、`ERROR`。
- `SendInitialEvents=true` 时先返回当前对象的 synthetic `ADDED`，再返回 `BOOKMARK`。
- `AllowBookmarks=true` 时允许发送 bookmark。
- `WatchMetadata` 只返回 metadata 变化。
- `WatchStatus` 只返回 status 变化。
- 普通 `Watch` 返回 object、metadata、status、deletion 的变化。

etcd 后端：

- etcd 继续使用原生 watch 作为唤醒机制。
- 事件回放仍走 cluster 自己的事件日志，保证 memory、badger、etcd 行为一致。
- 事件清理继续 master-only。

## 6. Selector 与索引

字段选择参考 Kubernetes：默认只支持稳定 metadata 字段，其他字段必须由 schema 显式声明。

默认允许：

- `metadata.name`
- `metadata.namespace`

schema 声明 `x-cluster-index: true` 后允许：

- `spec.<field>`
- `status.<field>`

metadata map 字段必须使用 `x-cluster-index-keys` 显式声明 key 后才允许：

- `metadata.labels.<key>`
- `metadata.annotations.<key>`

默认查询范围限定在已声明字段。未声明索引的字段用于 `Field(...)` 时返回 `ErrInvalidObject`。label selector 和 annotation selector 保留，annotation 的 required/default/immutable 改由 metadata schema 和 `x-cluster-immutable` 表达。

## 7. ResourceInfo 调整

Go SDK 资源发现结构改为：

```go
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
```

`ResourceInfo` 暴露完整 schema、index 和 admission 信息，资源发现以运行时实际定义为准。

## 8. Go Struct 生成

struct 生成只负责产生 JSON Schema，不参与运行时逻辑。

- `json` tag 决定字段名。
- 非 `omitempty` 字段进入 `required`。
- `omitempty` 字段不 required。
- 指针字段允许缺省；是否允许 `null` 由 tag 明确声明。
- 支持 struct、slice、array、map、pointer、string、bool、integer、number、time.Time、json.RawMessage、any。
- `time.Time` 生成 `type: string, format: date-time`。
- `map[string]T` 生成 `additionalProperties`。
- `json.RawMessage` / `any` 默认生成 preserve unknown 节点。
- `cluster` tag 只作为生成输入：`immutable`、`index`、`enum=a|b`、`default=...`、`nullable`、`preserveUnknown`。
- 运行时只读取 compiled JSON Schema。
- 可选字段必须使用 `omitempty` 或指针；非 `omitempty` 的 string/int/bool 会按 required 生成，这是 Kubernetes-style schema 的显式性要求。
- 生成结果必须是 structural schema，不生成缺失 `type` 的松散 schema。
- 结构体字段删除、字段类型变化、非 required 改 required、nullable 改 non-nullable 都视为不兼容变更。
- `omitempty` 到非 `omitempty` 会让字段变成 required，属于不兼容变更。
- 非指针字段加 `omitempty` 可以放宽 required，属于兼容变更。
- 新增带 `omitempty` 的字段是兼容变更。
- 新增不带 `omitempty` 的字段是 required 新增，属于不兼容变更。
- Go struct 生成要提供 schema 指纹，便于测试和重复 `Define` 检查同一个资源是否还是同一份结构。

## 9. Admission 内部资源

需要支持一类 Kubernetes admission 思路的资源检查。资源定义只做简单声明：哪些写入需要进入 admission。写入命中声明后，cluster 创建一个随机 key 的内部资源，保存这次写入的完整候选信息，并锁住目标对象；外部检查者通过 SDK 监听该内部资源，再调用 approve/reject API 决定这次写入是否提交。

JSON Schema 负责结构和静态约束，admission 负责需要业务状态、外部系统、跨对象检查的动态约束。

声明方式：

```go
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
```

规则：

- `Admission` 为空时写入直接走 schema 和内置校验。
- `AdmissionRule.Name` 必须在资源内唯一。
- `Operations` 为空表示匹配 `CREATE`、`UPDATE`、`DELETE`。
- `Patch`、`PatchMetadata`、`UpdateStatus`、`PatchStatus` 都按 `UPDATE` operation 匹配，再通过 `Subresource` 区分。
- `Subresources` 为空表示只匹配主资源；需要 status 或 metadata 时显式声明。
- 同一次写入如果匹配多个 rule，只创建一个 admission request，request 内包含所有待审批 rule。
- 泛型定义同样暴露 `Admission []AdmissionRule`，但 admission request 始终使用 unstructured 对象。

内部资源：

```go
const ResourceAdmissionRequests = "admissionrequests"

type AdmissionRequestSpec struct {
	Rules       []string
	Operation   AdmissionOperation
	Resource    string
	APIVersion  string
	Kind        string
	Namespaced  bool
	Namespace   string
	Name        string
	Subresource Subresource

	Precondition      AdmissionPrecondition
	SchemaFingerprint string
	OldObject         *Unstructured
	Object            *Unstructured
	EventAnnotations  Annotations

	CreatedByNode string
	ExpiresAt     time.Time
}

type AdmissionPrecondition struct {
	MustExist       bool
	MustNotExist    bool
	ResourceVersion string
}

type AdmissionPhase string

const (
	AdmissionPending  AdmissionPhase = "Pending"
	AdmissionRejected AdmissionPhase = "Rejected"
	AdmissionExpired  AdmissionPhase = "Expired"
	AdmissionCanceled AdmissionPhase = "Canceled"
	AdmissionCommitted AdmissionPhase = "Committed"
)

type AdmissionRuleDecision struct {
	Rule    string
	Message string
	Decider string
	At      time.Time
}

type AdmissionRequestStatus struct {
	Phase                 AdmissionPhase
	Approved              []AdmissionRuleDecision
	RejectedRule          string
	Message               string
	DecidedBy             string
	DecidedAt             time.Time
	LastError             string
	LastErrorAt           time.Time
	TargetResourceVersion string
	TargetObject          *Unstructured
}
```

内部资源语义：

- `metadata.name` 使用随机 key，例如 `adm-...`。
- admission request 是 cluster 内置资源，`apiVersion=cluster.d7z.net/v1`，`kind=AdmissionRequest`。
- admission request cluster-scoped；目标对象的 namespace 写在 `spec.namespace`。
- admission request 自身和其他内置控制面资源使用内置写入路径。
- `spec` 创建后保持稳定。
- 外部检查者通过 `Get/List/Watch` admission request 读取请求，并通过专用 API approve/reject。
- 专用决策 API 是 admission request 的唯一写入口。
- `status.phase` 表示 request 生命周期；分批审批进度通过 `status.approved` 表达。
- `status.approved` 记录每个 rule 的第一次 approve 决策，不因重复 approve 被覆盖。
- `status.rejectedRule` 只在 `Rejected` 终态设置。
- `status.message`、`status.decidedBy`、`status.decidedAt` 是终态摘要；`Committed` 使用最后一个 approve 决策，`Rejected` 使用 reject 决策，`Expired` / `Canceled` 使用 cluster 生成的终态原因。
- `status.targetResourceVersion` 和 `status.targetObject` 只在 `Committed` 终态设置。
- 原始写入方观察到 `Committed` 后直接使用 `status.targetObject` 作为返回值，不再依赖二次 `Get` 推断结果。
- `Committed`、`Rejected`、`Expired`、`Canceled` request 保留一段时间用于观察和排障，之后由 master-only cleanup 清理。
- terminal request 必须先满足最小保留时间，再参与按数量裁剪，避免原始写入方还未观察到终态就被删除。

新增 SDK API：

```go
func (c *Cluster) AdmissionRequests() *AdmissionRequestResource

type AdmissionDecisionOptions struct {
	Rule    string
	Message string
	Decider string
}

func (c *Cluster) ApproveAdmission(ctx context.Context, name string, opts AdmissionDecisionOptions) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
func (c *Cluster) RejectAdmission(ctx context.Context, name string, opts AdmissionDecisionOptions) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
```

只读 admission request 句柄：

```go
type AdmissionRequestResource struct{}

func (r *AdmissionRequestResource) Get(ctx context.Context, name string) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
func (r *AdmissionRequestResource) List(ctx context.Context, opts ListOptions) (*ObjectList[AdmissionRequestSpec, AdmissionRequestStatus], error)
func (r *AdmissionRequestResource) Watch(ctx context.Context, opts WatchOptions) (<-chan WatchEvent[AdmissionRequestSpec, AdmissionRequestStatus], error)
```

监听方式：

- 检查者通过 `c.AdmissionRequests().Watch(ctx, WatchOptions{SendInitialEvents: true})` 获取待处理请求。
- 检查者可以用 field selector 过滤：`spec.resource`、`spec.namespace`、`spec.name`、`spec.operation`、`status.phase`。
- 内置 `AdmissionRequest` schema 必须给这些字段声明 `x-cluster-index: true`。
- `ApproveAdmission` 支持按 rule 分批 approve；所有匹配 rule 都 approved 后才提交目标资源。
- `ApproveAdmission` 作用于 `Pending` request。
- 重复 approve 同一 rule 返回当前对象；第一次 approve 记录胜出，后续调用不覆盖 `status.approved`。
- 任意一个 rule rejected 后，本次写入失败并释放目标锁。
- `RejectAdmission` 作用于 `Pending` request；重复 reject 已终结 request 返回当前终结状态，便于检查者幂等重试。

写入执行顺序：

1. 构造候选对象，恢复 cluster 托管字段。
2. 应用 JSON Schema default。
3. 裁剪未知字段。
4. 执行内置静态校验：metadata、namespace、身份字段、status 子资源边界、不可变字段。
5. 计算匹配的 admission rules。
6. 如果没有匹配 rule，直接提交 store。
7. 如果有匹配 rule，生成随机 admission request key。
8. 在 store 内原子创建 admission request，并创建目标对象锁。
9. 原始写入调用等待 admission request 进入 `Committed` / `Rejected` / `Expired` / `Canceled`。
10. 最后一个 rule 被 approve 时，由 `ApproveAdmission` 负责在同一个 store 事务内重新校验 schema fingerprint、precondition、对象锁和候选对象，然后提交目标资源。
11. 目标资源提交成功后，同一事务把 admission request 状态改为 `Committed`，记录目标 `resourceVersion` 和 `targetObject`，释放目标锁。
12. 原始写入调用观察到 `Committed` 后返回 `status.targetObject`；观察到 `Rejected` / `Expired` / `Canceled` 后返回携带 request 信息和终态摘要的 admission 错误。
13. admission 采用同步阻塞模型；普通 `Create` / `Update` / `Patch` 等待 request 终态并返回最终结果。

目标锁：

- lock key 使用 `resource + namespace + name`。
- create 新对象时也要锁目标 identity，防止两个 pending create 同名对象。
- pending admission 期间，目标对象可以 read/list/watch；写请求返回携带 admission request name 的 `ErrAdmissionPending`。
- 其他写入命中同一目标锁时返回 `ErrAdmissionPending`，错误中应包含 admission request name。
- lock 必须持久化到 store；memory/badger 单实例也走同一抽象，etcd 用事务保证原子性。
- lock 带 `ExpiresAt`，超时后由 cleanup 释放，防止进程崩溃造成永久锁。
- delete admission 锁定的是本次 delete 的目标 identity。
- pending delete admission 期间，finalizer 变更、metadata 变更、status 变更都按普通写入命中同一把锁；审批流程不能依赖对被锁目标的后续写入才能完成。
- 检查者基于 request 中的 old object 和候选 object 做决策。

store 原子性要求：

- 创建 admission request、创建目标锁、检查目标 precondition 必须在同一个 store 事务里完成。
- approve 最后一个 rule 时，检查 request phase、检查目标锁归属、检查目标 precondition、提交目标资源、写目标事件、更新 request status、写入 `targetObject`、释放目标锁必须在同一个 store 事务里完成。
- reject / cancel / expire 时，更新 request status 和释放目标锁必须在同一个 store 事务里完成。
- 目标资源事件和 admission request 状态事件共享同一个全局 resourceVersion 序列；同一事务内先分配目标资源事件版本，再分配 admission request 状态事件版本。
- `approveAdmission` 事务返回已提交的目标对象和更新后的 admission request；等待中的原始写入调用通过 admission request 终态事件读取 `status.targetObject`。
- admission request 创建事件和目标锁创建不产生目标资源事件；锁不是用户可见资源。
- store 通过新增 multi-resource transaction 能力实现 admission 原子变更。
- store 接口按 admission 场景提供专用方法，保持事务能力收敛在内部实现：

```go
beginAdmission(context.Context, beginAdmissionRequest) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
approveAdmission(context.Context, approveAdmissionRequest) (*Unstructured, *Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
rejectAdmission(context.Context, rejectAdmissionRequest) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
expireAdmission(context.Context, expireAdmissionRequest) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error)
```

- memory 使用同一把 store mutex 完成事务。
- badger 使用同一个 update transaction 完成事务。
- etcd 使用一个 `Txn` 和 compare 条件完成事务；compare 至少包含 request phase、lock owner、目标对象当前值或不存在条件。
- 所有事务开始前检查 `ctx.Err()`；事务开始后按原子单元完成。

超时与取消：

- `Options.AdmissionTimeout` 控制一次 admission 最大等待时间，默认 30 秒。
- 命中的 rule 自带 `Timeout` 时，request deadline 使用所有正值 timeout 中的最小值；`spec.expiresAt` 固化为 `now + min(rule timeout, Options.AdmissionTimeout)`，后续 rule 变更不影响已创建 request。
- 原始写入 context 取消时，cluster 用条件事务把 still-pending request 标记为 `Canceled` 并释放目标锁；如果 request 已经 `Committed`，写入返回 `status.targetObject`。
- 超时未决时，admission request 标记为 `Expired`，释放目标锁，写入失败。
- 已经 canceled / expired / rejected 的 request 收到 approve 时返回当前终态对象。
- approval 本身不重试外部逻辑；如果最终提交时 precondition 不满足，request 标记为 `Expired`，释放锁，调用方重新发起写入。

提交约束：

- admission request 中保存的是已经 default、prune、内置校验后的完整候选对象，检查者看到的对象就是最终提交候选。
- 检查者通过 approve / reject 对候选对象做决策。
- approve 时重新执行 schema 校验和内置托管字段校验，保证提交节点的代码版本和 schema 与 request 匹配。
- approve 节点必须已经注册目标 resource definition；未注册或 definition 指纹不匹配时，approve 返回 `ErrInvalidResource`，request 保持 `Pending`，并在 `status.lastError` 记录原因。
- `status.lastError` 只在错误内容变化或时间间隔达到限流阈值时更新，避免检查者重试造成高频状态事件。
- `SchemaFingerprint` 必须和当前资源定义一致，否则拒绝提交。
- `Precondition.MustExist` 和 `Precondition.MustNotExist` 二选一。
- `Precondition.MustNotExist=true` 用于 create，approve 时目标仍必须不存在。
- `Precondition.MustExist=true` 和 `Precondition.ResourceVersion` 用于 update/patch/metadata/status/delete，approve 时目标仍必须存在且 RV 未变化。
- status 子资源 admission 只能提交 status 变化。
- metadata admission 只能提交 metadata 可写字段变化。
- delete admission 包含 old object 和候选结果；带 finalizer 的 delete 候选结果是设置 `deletedAt` 的对象，最终删除的候选结果是删除前的对象快照。
- `Committed` 的 `status.targetObject` 对 create/update/patch/metadata/status/finalizer delete 表示提交后的对象；对最终删除表示删除对象快照。

default / prune 算法：

- 注册 schema 时先解析本地 `$defs/$ref`，形成 compiled structural schema。
- default 和 prune 只按 resolved structural schema 执行。
- `allOf`、`oneOf`、`anyOf`、`not` 仅作为附加校验参与。
- default / prune 使用 resolved structural schema。
- schema 中声明的 default 在注册阶段先 prune 再 validate，编译后的 schema 保存裁剪后的 default。
- default 执行在 prune 前；default 产生的字段也必须通过 prune 和最终校验。
- admission request 保存 default / prune 后的候选对象。

清理：

- `Options.AdmissionRetentionCount` 控制保留的 `Committed` / `Rejected` / `Expired` / `Canceled` request 数量，默认 2000。
- `Options.AdmissionTerminalRetention` 控制 terminal request 的最小保留时间，默认 10 分钟。
- cleanup 只清理非 pending request。
- terminal request 同时满足超过最小保留时间和超过保留数量时才会被删除。
- pending request 超过 `ExpiresAt` 时先转为 `Expired` 并释放锁，再进入保留清理。
- etcd 多实例下 cleanup 仍然 master-only；memory/badger 单实例直接执行。

新增错误：

- `ErrAdmissionPending`：目标对象已有 pending admission，错误携带 admission request name。
- `ErrAdmissionRejected`：admission 被 reject，错误携带 admission request name、rule、message、decider、decidedAt。
- `ErrAdmissionExpired`：admission 超时，错误携带 admission request name、message、decidedAt。
- `ErrAdmissionCanceled`：写入 context 取消导致 admission 取消，错误携带 admission request name、message、decidedAt。

事件语义：

- admission request 自身的创建、状态变化走普通 watch 事件。
- 目标资源在 request pending 期间不产生资源事件。
- admission rejected / expired / canceled 不产生目标资源事件。
- admission committed 后只产生一次目标资源事件。

## 10. 实施顺序

- [x] 重构 schema 层：新增 schema 编译、默认值、裁剪、不可变、索引提取。
- [x] 替换资源定义 API：新增非泛型 `ResourceDef`，泛型变成 wrapper。
- [x] 增加 structural schema 检查和 schema 指纹。
- [x] 增加重复 `Define` 检查：同定义允许，变更定义拒绝。
- [x] 增加 admission request 内部资源、目标锁和 approve/reject API。
- [x] 扩展 store multi-resource transaction，用于 admission request、lock、目标资源提交的原子变更。
- [x] 将 selector、metadata 约束和 admission 声明统一接入 schema 编译结果。
- [x] 改 CRUD、patch、metadata、status 校验流程，使其全部走 compiled schema 和 admission request。
- [x] 改 selector：未声明 index 的字段选择直接拒绝。
- [x] 改 `ResourceInfo` 和资源发现数据结构。
- [x] 更新 builtin node/master/admissionrequests schema。
- [x] 更新 README 和 common 英文示例。
- [x] 更新测试到 `ResourceDef`、JSON Schema 和 admission API。
- [x] 增加 memory/badger/etcd 黑盒测试。

## 11. 测试矩阵

Schema：

- required、enum、type、format、default。
- unknown fields 默认裁剪。
- preserve unknown 节点保留。
- 非 structural schema 注册失败。
- default 不符合 schema 注册失败。
- immutable spec 字段 create 后不可修改。
- status 下声明 immutable 被拒绝。
- index 字段允许 field selector。
- `x-cluster-index-keys` 允许指定 labels / annotations key。
- `x-cluster-index-keys` 出现在非 metadata labels / annotations 节点时注册失败。
- 未声明 index 字段使用 field selector 被拒绝。

资源定义：

- 同名资源重复注册相同 schema 允许或返回已有定义。
- 同名资源重复注册不同 schema 拒绝。
- `Resource`、`APIVersion`、`Kind`、`Namespaced` 冲突拒绝。
- Go struct 生成的 schema 指纹稳定。
- Go struct 删除字段、改变字段类型、新增 required 字段在兼容检查中判定为不兼容。

Admission：

- create/update/patch/metadata/status/delete 命中 rule 时都会创建 admission request。
- admission request 使用随机 key，spec 保存 old object、候选 object、precondition、schema fingerprint、event annotations。
- `AdmissionRequests()` 只暴露 get/list/watch，approve/reject 是 admission request 的决策入口。
- pending admission 会锁住目标对象，其他写入返回 `ErrAdmissionPending`。
- read/list/watch 不受目标锁影响。
- approve 所有匹配 rule 后才提交目标资源。
- 最后一个 approve 会在同一 store 事务内提交目标资源、更新 request 状态、释放目标锁。
- committed request 的 `status.targetObject` 是原始写入调用返回的对象。
- 最终删除 committed request 的 `status.targetObject` 是删除对象快照。
- approve 节点未注册目标资源定义时保持 pending 并返回错误。
- approve 节点未注册目标资源定义时写入 `status.lastError`，并对重复错误更新限流。
- 任意 rule reject 后写入失败且不产生目标资源事件。
- `ErrAdmissionPending`、`ErrAdmissionRejected`、`ErrAdmissionExpired`、`ErrAdmissionCanceled` 携带 admission request name 和终态摘要。
- 重复 approve/reject 已处理状态幂等返回当前对象。
- 重复 approve 同一 rule 保留第一次 decision。
- context 取消会把 request 标记为 canceled 并释放锁。
- context 取消和 approve 并发时结果只能是 canceled 或 committed 二选一，不能出现目标已提交但 request 仍 pending。
- admission timeout 会把 request 标记为 expired 并释放锁。
- approve 时 schema fingerprint 不一致会失败。
- create 的 precondition 必须保证目标仍不存在。
- update/patch/metadata/status/delete 的 precondition 必须保证目标 RV 未变化。
- approve 后目标资源只产生一次 committed 事件。
- status 子资源 admission 不能提交 spec 变化。
- delete 有 finalizer 和真正 delete 都创建 admission request。
- committed / rejected / expired / canceled request 满足最小保留时间和数量条件后由 cleanup 清理。
- 高并发终态 request 清理不会影响仍在等待终态事件的原始写入调用。
- admission request 状态事件和目标资源事件使用同一个全局 RV 序列，顺序稳定。
- committed 返回对象、最终删除快照、终态错误载荷、terminal cleanup 竞态都必须有黑盒测试覆盖。

CRUD：

- create/update/patch 均校验最终对象。
- main resource 保持 status 不变。
- create/update/patch 携带非空或变化的 status 被拒绝。
- status 方法只能改 status。
- metadata name/namespace/uid/resourceVersion/createdAt 不可变。
- generation 仅 spec 变化递增。
- finalizer 删除流程。
- CAS 冲突。

Namespace：

- namespaced resource 必须通过 namespace 句柄写入。
- namespaced object 的 namespace 在 patch/update 中保持不变。
- cluster-scoped resource 的 namespace 保持为空。
- namespaced resource 的全 namespace list/watch 正常。

Watch：

- watch 从 list resourceVersion 之后接续。
- 太旧 resourceVersion 返回 `ErrResourceVersionTooOld`。
- bookmark。
- sendInitialEvents。
- namespaced watch 不接收其他 namespace 事件。
- status 修改只触发普通 watch 和 status watch，不触发 metadata watch。
- metadata 修改只触发普通 watch 和 metadata watch，不触发 status watch。

后端：

- memory URL 黑盒测试。
- badger URL 黑盒测试。
- etcd URL 黑盒测试。
- etcd watch 仍使用原生 watch 唤醒。
- event cleanup 仍只由 master 执行。

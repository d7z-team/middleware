# cluster 使用说明

## 创建 Cluster

```go
c, err := cluster.NewClusterFromURL("memory://?node=worker-a")
if err != nil {
	return err
}
defer c.Close()
```

支持的 URL：

- `memory://?node=worker-a`
- `mem://?node=worker-a`
- `badger:///var/lib/app/cluster?node=worker-a&prefix=app`
- `etcd://127.0.0.1:2379?node=worker-a&prefix=app`

支持的参数：

- `node`：当前实例 node 名称，必填。
- `prefix`：badger / etcd 存储前缀。
- `node_lease_ttl`：node lease TTL，默认 `30s`。
- `node_renew_interval`：node 续约间隔，默认 `10s`。
- `master_lease_ttl`：master lease TTL，默认跟随 `node_lease_ttl`。
- `master_renew_interval`：master 续约间隔，默认跟随 `node_renew_interval`。
- `master_history_limit`：master 切换历史保留数量，默认 `2000`。
- `event_retention_count`：watch 事件保留数量，默认 `2000`。
- `event_cleanup_interval`：watch 事件清理间隔。
- `admission_timeout`：单次 admission 同步等待超时，默认 `30s`。
- `admission_retention_count`：终态 admission request 保留数量，默认 `2000`。
- `admission_terminal_retention`：终态 admission request 最小保留时间，默认 `10m`。
- `watch_buffer_size`：每个 watch channel 缓冲区大小，默认 `256`。

## 定义资源

推荐通过 Go struct 生成 schema，再注册 typed resource：

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index"`
}

type WidgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index"`
}

widgets, err := cluster.Define(c, cluster.TypedResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
})
if err != nil {
	return err
}
```

也可以先生成 schema，再用 unstructured 方式注册：

```go
schema, err := cluster.SchemaFrom[WidgetSpec, WidgetStatus]("example.test/v1", "Widget", false)
if err != nil {
	return err
}

rawWidgets, err := cluster.DefineResource(c, cluster.ResourceDef{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
	Schema:     schema,
})
if err != nil {
	return err
}
_ = rawWidgets
```

`cluster` tag 支持：

- `required`
- `enum=a|b|c`
- `immutable`
- `index`
- `default=value`
- `nullable`
- `preserveUnknown`

## Schema 规则

当前运行时只认 `JSON Schema`，typed API 只是 schema 生成和 typed wrapper。

结构约束：

- root 必须是 `object`。
- root 必须包含 `apiVersion`、`kind`、`metadata`、`spec`。
- `status` 可选；未声明时按空 `object` 处理。
- 每个 object 节点都必须有明确 `type`。
- 每个 array 节点都必须声明 `items`。
- map 必须通过 `additionalProperties` 声明 value schema。
- 只允许本地 `$ref`，即 `#/$defs/...`。
- 默认使用 structural schema 思路，不允许依赖隐式自由结构。

当前实现的 cluster 扩展：

- `x-cluster-immutable`
- `x-cluster-index`
- `x-cluster-index-keys`
- `x-cluster-preserve-unknown-fields`
- `x-cluster-metadata-writable`

扩展含义：

- `x-cluster-immutable`：字段创建后不可变；不允许用于 `status.*`。
- `x-cluster-index`：字段可用于 field selector；只允许稳定 scalar 字段。
- `x-cluster-index-keys`：只允许出现在 `metadata.labels` 和 `metadata.annotations` 上，声明允许查询的 key。
- `x-cluster-preserve-unknown-fields`：当前节点关闭未知字段裁剪。
- `x-cluster-metadata-writable`：标记 metadata 子字段允许通过主资源或 metadata 子资源写入；当前内置只给 `labels`、`annotations`、`finalizers`。

默认值与裁剪：

- schema 中的 `default` 会在注册时先进入编译结果。
- 写入时先应用 default，再做 prune。
- 未开启 `x-cluster-preserve-unknown-fields` 的节点，未知字段会被裁剪。
- `spec` 和 `status` 都走同一套 default/prune 机制。

## Go Struct 到 Schema 的映射

`SchemaFrom` 和 `Define` 的泛型路径当前按下面规则生成 schema：

- `json` tag 决定字段名。
- `omitempty` 会让字段不进入 `required`。
- 非 `omitempty` 字段进入 `required`。
- 指针字段默认不 required。
- `time.Time` 生成 `type: string, format: date-time`。
- `map[string]T` 生成 `additionalProperties`。
- `json.RawMessage` 和 `any` 默认生成 preserve-unknown 节点。
- `cluster:"index"` 生成 `x-cluster-index`。
- `cluster:"immutable"` 生成 `x-cluster-immutable`。
- `cluster:"enum=a|b|c"` 生成 `enum`。
- `cluster:"default=value"` 生成 `default`。
- `cluster:"preserveUnknown"` 生成 `x-cluster-preserve-unknown-fields`。

注意：

- `cluster:"required"` 当前只作为显式说明使用；真正的 required 由 `omitempty` / 指针语义决定。
- `cluster:"nullable"` 当前只作为生成输入保留，运行时没有单独做标准 JSON Schema `null` 语义扩展。
- `default=value` 当前按字符串值写入 schema；不是通用 JSON 解码器。

## Metadata 规则

内置 metadata 语义：

- `metadata.name` 创建时指定，之后不可变。
- namespaced 资源的 `metadata.namespace` 由 `Namespace("x")` 句柄决定，之后不可变。
- cluster-scoped 资源不能带 namespace。
- `uid`、`resourceVersion`、`generation`、`createdAt`、`updatedAt`、`deletedAt` 全部由 cluster 管理。
- `labels`、`annotations`、`finalizers` 是当前唯一允许写入的 metadata 字段。

写入约束：

- `PatchMetadata` 只允许改 `labels`、`annotations`、`finalizers`。
- 主资源 `Create/Update/Patch` 也只能修改这三类 metadata 可写字段。
- `deletedAt` 只能通过 `Delete` 路径进入。
- metadata key 不能为空，不能包含 `\x00`。

## Status 与 Generation 规则

- `status` 只能通过 `UpdateStatus` / `PatchStatus` 修改。
- `Create` 的 `status` 必须为空。
- 主资源 `Update` / `Patch` 携带的 `status` 必须和旧对象完全一致。
- `generation` 只在 `spec` 变化时递增。
- 仅 metadata / status 变化不会递增 `generation`。

## Selector 规则

当前 field selector 的允许范围：

- 默认允许 `metadata.name`。
- namespaced 资源额外允许 `metadata.namespace`。
- schema 声明了 `x-cluster-index` 的 `spec.*` 和 `status.*` 字段允许查询。
- schema 声明了 `x-cluster-index-keys` 的 `metadata.labels.<key>` 和 `metadata.annotations.<key>` 允许查询。

限制：

- 未声明索引的字段用于 `Field(...)` 会直接返回 `ErrInvalidObject`。
- object 和 array 不能直接声明 `x-cluster-index`。

## 写入执行顺序

主资源 `Create/Update/Patch` 的运行时顺序：

1. 恢复托管字段和身份字段。
2. 应用 schema default。
3. 执行 prune，裁剪未知字段。
4. 执行 metadata / spec / status 边界校验。
5. 执行 immutable / required / enum 等编译后的字段规则。
6. 计算是否命中 admission。
7. 未命中则直接提交；命中则进入 admission request。

`UpdateStatus` / `PatchStatus` 只对 `status` 走对应校验和 admission。  
`PatchMetadata` 只对 metadata 可写字段走对应校验和 admission。

## Admission 规则

`AdmissionRule` 语义：

- `Name` 在同一资源内必须唯一。
- `Operations` 为空时表示匹配 `CREATE`、`UPDATE`、`DELETE`。
- `Subresources` 为空时只匹配主资源。
- `Patch`、`PatchMetadata`、`UpdateStatus`、`PatchStatus` 都按 `UPDATE` 进入，再由 `Subresource` 区分。

运行时行为：

- 命中 admission 的写入会创建内置资源 `admissionrequests`。
- request 是 cluster-scoped，目标对象 namespace 记录在 `spec.namespace`。
- 同一目标对象在 pending admission 期间会被锁住。
- 其他写入命中同一锁时返回 `ErrAdmissionPending`。
- 原始写入方会同步等待 request 终态。
- 所有规则都 approve 后才会真正提交目标资源。
- 任一规则 reject 后写入失败，目标资源不会产生提交事件。
- request 终态会保留一段时间，再按数量和最小保留时间清理。

终态：

- `Pending`
- `Committed`
- `Rejected`
- `Expired`
- `Canceled`

## Watch 规则

- `Watch` 返回对象整体变化，包括 spec、metadata、status、删除态变化。
- `WatchMetadata` 只返回 metadata 变化。
- `WatchStatus` 只返回 status 变化。
- `Since` 太旧时返回 `ErrResourceVersionTooOld`。
- `SendInitialEvents=true` 会先发当前对象的 synthetic `ADDED`，然后发 `BOOKMARK`。
- `AllowBookmarks=true` 允许返回 bookmark 事件。

## 当前未支持范围

当前实现不是完整通用 JSON Schema 引擎，下面这些不要按完整 draft 语义理解：

- 没有接第三方通用 JSON Schema 校验库。
- 不是完整的 Draft 2020-12 全量实现。
- `oneOf`、`anyOf`、`allOf`、`not` 没有作为完整运行时通用校验入口接入。
- `cluster:"default=value"` 不是通用类型化 default 解析器。
- `nullable` 没有扩展成完整 `null` 运行时语义体系。

## 命名空间

默认资源是 cluster-scoped。定义时设置 `Namespaced: true` 后，必须先绑定 namespace：

```go
jobs, err := cluster.Define(c, cluster.TypedResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "jobs",
	APIVersion: "example.test/v1",
	Kind:       "Job",
	Namespaced: true,
})
if err != nil {
	return err
}

billingJobs, err := jobs.Namespace("billing")
if err != nil {
	return err
}

allJobs, err := jobs.AllNamespaces()
if err != nil {
	return err
}
_ = allJobs
```

- `Namespace("x")` 用于 namespaced 资源的读写。
- `AllNamespaces()` 只用于 `List` 和 `Watch`。

## CRUD

```go
created, err := widgets.Create(ctx, "alpha", WidgetSpec{
	Owner: "team-a",
}, cluster.CreateOptions{
	Labels:      cluster.Labels{"app": "demo"},
	Annotations: cluster.Annotations{"team": "platform"},
})
if err != nil {
	return err
}

got, err := widgets.Get(ctx, "alpha")
if err != nil {
	return err
}

updated, err := widgets.Update(ctx, &cluster.Object[WidgetSpec, WidgetStatus]{
	APIVersion: got.APIVersion,
	Kind:       got.Kind,
	Metadata:   got.Metadata,
	Spec: WidgetSpec{
		Size:  "large",
		Owner: got.Spec.Owner,
	},
	Status: got.Status,
}, cluster.UpdateOptions{
	ResourceVersion: got.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

patched, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"small"}}`), cluster.PatchOptions{
	ResourceVersion: updated.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

metaPatched, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), cluster.PatchOptions{
	ResourceVersion: patched.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = widgets.UpdateStatus(ctx, "alpha", WidgetStatus{
	Phase: "Ready",
}, cluster.UpdateOptions{
	ResourceVersion: metaPatched.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = widgets.PatchStatus(ctx, "alpha", []byte(`{"phase":"Failed"}`), cluster.PatchOptions{
	ResourceVersion: metaPatched.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = widgets.Delete(ctx, "alpha", cluster.DeleteOptions{})
if err != nil {
	return err
}
```

规则：

- `Create` 创建完整对象。
- `Update` 更新主资源，不允许通过主资源改 `status`。
- `Patch` 使用 JSON merge patch。
- `PatchMetadata` 只允许改 `labels`、`annotations`、`finalizers`。
- `UpdateStatus` / `PatchStatus` 只修改 `status`。
- `resourceVersion` 是 CAS 条件；传入时必须精确匹配。
- 有 finalizer 的对象第一次删除只会设置 `deletedAt`，finalizer 清空后再次删除才会真正移除。

## List 与 Selector

```go
list, err := widgets.List(ctx, cluster.ListOptions{
	Selector: cluster.Where(
		cluster.Field("metadata.name").Eq("alpha"),
		cluster.Field("spec.owner").Eq("team-a"),
		cluster.Field("status.phase").Eq("Ready"),
		cluster.Label("app").Eq("demo"),
		cluster.Annotation("team").Eq("platform"),
	),
})
if err != nil {
	return err
}
```

规则：

- 默认允许 `metadata.name`。
- namespaced 资源额外允许 `metadata.namespace`。
- `spec.*`、`status.*` 必须在 schema 中声明 `x-cluster-index`，或通过 `cluster:"index"` 生成。
- `metadata.labels.<key>` 和 `metadata.annotations.<key>` 必须由 schema 显式声明可索引 key。
- 未声明索引的字段用于 `Field(...)` 会直接返回 `ErrInvalidObject`。

## Watch

```go
events, err := widgets.Watch(ctx, cluster.WatchOptions{
	Since:             list.ResourceVersion,
	AllowBookmarks:    true,
	SendInitialEvents: true,
})
if err != nil {
	return err
}

metadataEvents, err := widgets.WatchMetadata(ctx, cluster.WatchOptions{
	Since: list.ResourceVersion,
})
if err != nil {
	return err
}

statusEvents, err := widgets.WatchStatus(ctx, cluster.WatchOptions{
	Since: list.ResourceVersion,
})
if err != nil {
	return err
}

_, _, _ = events, metadataEvents, statusEvents
```

说明：

- `Watch` 返回对象整体变化。
- `WatchMetadata` 只返回 metadata 变化。
- `WatchStatus` 只返回 status 变化。
- 支持 `ADDED`、`MODIFIED`、`DELETED`、`BOOKMARK`、`ERROR`。
- `SendInitialEvents=true` 会先返回当前对象的 synthetic `ADDED`，再返回 `BOOKMARK`。

## Admission

资源可以声明需要外部审批的写入：

```go
guardedWidgets, err := cluster.Define(c, cluster.TypedResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "guardedwidgets",
	APIVersion: "example.test/v1",
	Kind:       "GuardedWidget",
	Admission: []cluster.AdmissionRule{
		{
			Name:       "create-check",
			Operations: []cluster.AdmissionOperation{cluster.AdmissionCreate},
		},
		{
			Name:         "metadata-check",
			Operations:   []cluster.AdmissionOperation{cluster.AdmissionUpdate},
			Subresources: []cluster.Subresource{cluster.SubresourceMetadata},
		},
	},
})
if err != nil {
	return err
}
```

监听和审批 admission request：

```go
requests, err := c.AdmissionRequests().Watch(ctx, cluster.WatchOptions{
	SendInitialEvents: true,
})
if err != nil {
	return err
}

for event := range requests {
	if event.Object == nil || event.Object.Status.Phase != cluster.AdmissionPendingPhase {
		continue
	}
	_, err := c.ApproveAdmission(ctx, event.Object.Metadata.Name, cluster.AdmissionDecisionOptions{
		Rule:    "create-check",
		Decider: "controller-a",
		Message: "approved",
	})
	if err != nil {
		return err
	}
}
```

相关接口：

- `c.AdmissionRequests().Get/List/Watch`
- `c.ApproveAdmission(...)`
- `c.RejectAdmission(...)`

## 资源发现

```go
info, err := c.Resource("widgets")
if err != nil {
	return err
}

resources, err := c.Resources()
if err != nil {
	return err
}

_, _ = info, resources
```

`ResourceInfo` 暴露：

- `Resource`
- `APIVersion`
- `Kind`
- `Namespaced`
- `Schema`
- `Indexes`
- `Admission`
- `Builtin`

## Node API

内置 `nodes` 资源会自动注册当前连接实例自身：

```go
node, err := c.CurrentNode(ctx)
if err != nil {
	return err
}

patchedMeta, err := c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"role":"worker"}}`), cluster.PatchOptions{
	ResourceVersion: node.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

patchedSpec, err := c.PatchCurrentNodeSpec(ctx, []byte(`{"metadata":{"zone":"cn-sh-1"}}`), cluster.PatchOptions{
	ResourceVersion: patchedMeta.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

patchedStatus, err := c.PatchCurrentNodeStatus(ctx, []byte(`{"metadata":{"ready":"true"}}`), cluster.PatchOptions{
	ResourceVersion: patchedSpec.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = c.UpdateCurrentNodeStatus(ctx, cluster.NodeStatus{
	Metadata: cluster.Annotations{"ready": "true"},
}, cluster.UpdateOptions{
	ResourceVersion: patchedStatus.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

## Master API

内置 `masters` 资源用于 master 选举和切换历史：

```go
master, err := c.Master(ctx)
if err != nil {
	return err
}

isMaster, err := c.IsMaster(ctx)
if err != nil {
	return err
}

history, err := c.MasterHistory(ctx, 20)
if err != nil {
	return err
}

watch, err := c.WatchMaster(ctx, cluster.WatchOptions{
	Since: master.ResourceVersion,
})
if err != nil {
	return err
}

_, _, _, _ = master, isMaster, history, watch
```

## Unstructured API

如果不需要 typed wrapper，可以直接使用 unstructured handle：

```go
raw, err := c.Unstructured("widgets")
if err != nil {
	return err
}

obj, err := raw.Get(ctx, "alpha")
if err != nil {
	return err
}

_ = obj
```

支持的能力与 typed resource 一致，包括：

- `Get`
- `List`
- `Watch`
- `Create`
- `Update`
- `Patch`
- `PatchMetadata`
- `UpdateStatus`
- `PatchStatus`
- `Delete`
- `Namespace`
- `AllNamespaces`

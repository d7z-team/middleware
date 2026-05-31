# cluster 使用说明

## 创建 Cluster

```go
c, err := cluster.NewClusterFromURL("memory://?node=worker-a&watch_buffer_size=256")
if err != nil {
	return err
}
defer c.Close()
```

可用 URL：

| URL | 用途 |
| --- | --- |
| `memory://?node=worker-a` | 进程内存储 |
| `mem://?node=worker-a` | `memory://` 的简写 |
| `badger:///path/to/db?node=worker-a&prefix=app` | 本地持久化存储 |
| `etcd://127.0.0.1:2379?node=worker-a&prefix=app` | etcd 存储 |

可用参数：

| 参数 | 说明 |
| --- | --- |
| `node` | 当前实例的 node 名称，必填，同一 backend/prefix 下不能重复 |
| `prefix` | 存储前缀 |
| `node_lease_ttl` | node lease TTL，默认 `30s` |
| `node_renew_interval` | node lease 续约间隔，默认 `10s`，必须小于 TTL，传入时不得小于 `10ms` |
| `master_lease_ttl` | master lease TTL，默认跟随 `node_lease_ttl` |
| `master_renew_interval` | master lease 续约间隔，默认跟随 `node_renew_interval`，必须小于 master TTL，传入时不得小于 `10ms` |
| `master_history_limit` | 保留的 master 切换记录数量，默认 `2000`，传入时必须大于 `0` |
| `event_retention_count` | 保留的 watch 事件数量，默认 `2000`，传入时必须大于 `0` |
| `event_cleanup_interval` | 当前 master 清理旧 watch 事件的间隔，默认跟随 `master_renew_interval`，传入时不得小于 `10ms` |
| `watch_buffer_size` | 每个 watch channel 的缓冲大小，默认 `256` |

`etcd` 后端的资源写入、node lease 和 master lease 都落在 etcd 中，多个实例共享同一
`prefix` 时通过 etcd 的 CAS 和 watch 协调，最终一致性由 etcd 自身的 Raft 保证。
`memory` 和 `badger` 用于单实例，本地进程内的 watch 通过内存锁和事件缓冲实现。

## 定义资源

每类资源需要先调用 `Define`。`S` 是 `spec` 类型，`T` 是 `status` 类型。

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type WidgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase"`
}

widgets, err := cluster.Define(c, cluster.ResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
	Annotations: []cluster.AnnotationRule{
		{Key: "tenant", Required: true, Immutable: true, Indexed: true},
		{Key: "controller", Default: "default-controller", Indexed: true},
	},
	Default: func(obj *cluster.Object[WidgetSpec, WidgetStatus]) error {
		if obj.Spec.Size == "" {
			obj.Spec.Size = "medium"
		}
		return nil
	},
	Validate: func(oldObj, newObj *cluster.Object[WidgetSpec, WidgetStatus], sub cluster.Subresource) error {
		if sub == cluster.SubresourceStatus {
			return nil
		}
		if oldObj != nil && newObj.Spec.Owner != oldObj.Spec.Owner {
			return cluster.ErrInvalidObject
		}
		return nil
	},
})
if err != nil {
	return err
}
```

`ResourceDef` 字段：

| 字段 | 说明 |
| --- | --- |
| `Resource` | 资源复数名，例如 `widgets` |
| `APIVersion` | 对象的 `apiVersion` |
| `Kind` | 对象的 `kind` |
| `Namespaced` | `true` 表示资源按 namespace 隔离；默认 `false` 表示 cluster-scoped |
| `Annotations` | metadata annotation 规则 |
| `Default` | 创建或更新 spec 时填默认值 |
| `Validate` | 创建、更新 spec、更新 metadata、更新 status 时做校验 |

资源名不能是空值、`.`、`..`，也不能包含 `/` 或 `\`。

## 命名空间

默认资源是 cluster-scoped。需要命名空间隔离时，在定义资源时设置 `Namespaced: true`：

```go
widgets, err := cluster.Define(c, cluster.ResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
	Namespaced: true,
})
if err != nil {
	return err
}
```

命名空间资源必须先绑定 namespace 才能写入、读取或删除单个对象：

```go
teamWidgets, err := widgets.Namespace("team-a")
if err != nil {
	return err
}

created, err := teamWidgets.Create(ctx, "alpha", WidgetSpec{
	Size:  "small",
	Owner: "team-a",
}, cluster.CreateOptions{})
if err != nil {
	return err
}
_ = created.Metadata.Namespace // "team-a"

got, err := teamWidgets.Get(ctx, "alpha")
if err != nil {
	return err
}
_ = got
```

同一个资源名在不同 namespace 下可以使用相同对象名；对象身份由
`resource + namespace + name` 组成。`Metadata.Namespace` 由 namespace 句柄写入，
创建后不可修改；`Patch`、`PatchMetadata`、`Update` 都不能改变 namespace。

未绑定 namespace 的命名空间资源可以 `List` 和 `Watch` 全部 namespace。
也可以显式使用 `AllNamespaces()`，该句柄只允许 `List` 和 `Watch`：

```go
allWidgets, err := widgets.AllNamespaces()
if err != nil {
	return err
}

list, err := allWidgets.List(ctx, cluster.ListOptions{
	Selector: cluster.Where(cluster.Field("metadata.namespace").Eq("team-a")),
})
if err != nil {
	return err
}
_ = list
```

cluster-scoped 资源不能调用 `Namespace` 或 `AllNamespaces`。namespace 名称不能是空值、
`.`、`..`，也不能包含 `/` 或 `\`。

## 字段规则

字段规则写在 `spec` 或 `status` 的导出字段 tag 上：

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}
```

支持的 tag：

| tag | 说明 |
| --- | --- |
| `required` | 字段不能为空 |
| `enum=a\|b\|c` | 字段值必须在枚举中 |
| `immutable` | 对象创建后字段不可修改 |
| `index` | 声明字段用于查询 |
| `index=name` | 声明字段索引名 |

字段路径按 JSON 字段名生成，例如 `Size` 的路径是 `spec.size`，`Phase` 的路径是
`status.phase`。

## 注解

metadata 注解通过 `CreateOptions.Annotations` 写入：

```go
created, err := widgets.Create(ctx, "annotated", WidgetSpec{
	Size:  "small",
	Owner: "team-a",
}, cluster.CreateOptions{
	Labels: cluster.Labels{"app": "demo"},
	Annotations: cluster.Annotations{
		"tenant": "t1",
		"owner":  "billing",
	},
})
if err != nil {
	return err
}
```

注解规则在 `Define` 时声明：

```go
Annotations: []cluster.AnnotationRule{
	{Key: "tenant", Required: true, Immutable: true, Indexed: true},
	{Key: "controller", Default: "default-controller", Indexed: true},
},
```

规则字段：

| 字段 | 说明 |
| --- | --- |
| `Key` | annotation key |
| `Required` | 创建和更新时必须存在且非空 |
| `Immutable` | 创建后不能修改 |
| `Default` | 未提供时填入默认值 |
| `Indexed` | 声明注解用于查询 |

修改 metadata 使用 `PatchMetadata`：

```go
patched, err := widgets.PatchMetadata(ctx, created.Metadata.Name, []byte(`{
	"labels": {
		"app": "demo"
	},
	"annotations": {
		"owner": "platform"
	}
}`), cluster.PatchOptions{
	ResourceVersion: created.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

注解可以用于 `List` 和 `Watch` 过滤：

```go
list, err := widgets.List(ctx, cluster.ListOptions{
	Selector: cluster.Where(
		cluster.Annotation("tenant").Eq("t1"),
		cluster.Annotation("owner").Eq("platform"),
	),
})
if err != nil {
	return err
}
```

事件注解通过 `EventAnnotations` 传入。事件注解只出现在 watch 事件中，不会写入
`metadata.annotations`：

```go
_, err = widgets.Patch(ctx, patched.Metadata.Name, []byte(`{"spec":{"size":"large"}}`), cluster.PatchOptions{
	ResourceVersion: patched.Metadata.ResourceVersion,
	EventAnnotations: cluster.Annotations{
		"reason": "resize",
	},
})
```

## 创建和读取对象

```go
created, err := widgets.Create(ctx, "alpha", WidgetSpec{
	Size:  "small",
	Owner: "team-a",
}, cluster.CreateOptions{
	Labels:      cluster.Labels{"app": "demo"},
	Annotations: cluster.Annotations{"tenant": "t1"},
})
if err != nil {
	return err
}

got, err := widgets.Get(ctx, created.Metadata.Name)
if err != nil {
	return err
}
```

对象包含这些托管字段：

| 字段 | 说明 |
| --- | --- |
| `Metadata.UID` | 创建时生成，不可修改 |
| `Metadata.Namespace` | namespaced 资源的 namespace，cluster-scoped 资源为空，创建后不可修改 |
| `Metadata.ResourceVersion` | 每次成功写入后递增 |
| `Metadata.Generation` | `spec` 变化时递增 |
| `Metadata.CreatedAt` | 创建时间 |
| `Metadata.UpdatedAt` | 最近更新时间 |
| `Metadata.DeletedAt` | finalizer 删除流程中的删除时间 |

## 更新 spec

`Update` 传入完整对象，适合已经通过 `Get` 读取并修改后的场景：

```go
got.Spec.Size = "medium"
updated, err := widgets.Update(ctx, got, cluster.UpdateOptions{
	ResourceVersion: got.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

`Patch` 使用 JSON merge patch，适合只修改部分字段：

```go
patched, err := widgets.Patch(ctx, updated.Metadata.Name, []byte(`{
	"spec": {"size": "large"},
	"metadata": {
		"labels": {"app": "demo"},
		"annotations": {"owner": "platform"}
	}
}`), cluster.PatchOptions{
	ResourceVersion: updated.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

普通 `Update` 和 `Patch` 只能修改 `metadata.labels`、`metadata.annotations`、
`metadata.finalizers` 和 `spec`。推荐使用 `PatchMetadata` 修改 metadata，
`PatchMetadata` 不会递增 `Metadata.Generation`。`status` 需要使用 status 方法修改。

## 更新 status

```go
statused, err := widgets.UpdateStatus(ctx, patched.Metadata.Name, WidgetStatus{
	Phase: "Ready",
}, cluster.UpdateOptions{
	ResourceVersion: patched.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

部分修改 status：

```go
statused, err = widgets.PatchStatus(ctx, statused.Metadata.Name, []byte(`{
	"phase": "Failed"
}`), cluster.PatchOptions{
	ResourceVersion: statused.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

status 更新不会递增 `Metadata.Generation`。

## 列表和分页

```go
list, err := widgets.List(ctx, cluster.ListOptions{
	Limit: 100,
	Selector: cluster.Where(
		cluster.Label("app").In("demo", "worker"),
		cluster.Annotation("tenant").Eq("t1"),
		cluster.Annotation("tenant").Exists(),
		cluster.Field("status.phase").Eq("Ready"),
	),
})
if err != nil {
	return err
}

for _, item := range list.Items {
	_ = item.Metadata.Name
}

for list.Continue != "" {
	list, err = widgets.List(ctx, cluster.ListOptions{
		Limit:    100,
		Continue: list.Continue,
	})
	if err != nil {
		return err
	}
}
```

支持的 selector：

| 写法 | 说明 |
| --- | --- |
| `cluster.Label("app").Eq("demo")` | 匹配 label |
| `cluster.Label("app").NotEq("demo")` | label 不等于指定值，缺失也会匹配 |
| `cluster.Label("app").In("api", "worker")` | label 在集合内 |
| `cluster.Label("app").NotIn("api", "worker")` | label 不在集合内，缺失也会匹配 |
| `cluster.Label("app").Exists()` | label 存在 |
| `cluster.Annotation("tenant").Eq("t1")` | 匹配 annotation |
| `cluster.Field("metadata.name").Eq("alpha")` | 匹配 metadata 字段 |
| `cluster.Field("metadata.namespace").Eq("team-a")` | 匹配 namespace |
| `cluster.Field("spec.size").Eq("large")` | 匹配 spec 字段 |
| `cluster.Field("status.phase").Eq("Ready")` | 匹配 status 字段 |

字段 selector 支持：

- `metadata.name`
- `metadata.namespace`
- `metadata.uid`
- `apiVersion`
- `kind`
- `spec.<field>`
- `status.<field>`

## Watch

从一次 `List` 的版本之后开始 watch：

```go
list, err := widgets.List(ctx, cluster.ListOptions{
	Selector: cluster.Where(cluster.Annotation("tenant").Eq("t1")),
})
if err != nil {
	return err
}

events, err := widgets.Watch(ctx, cluster.WatchOptions{
	Since:          list.ResourceVersion,
	Selector:       cluster.Where(cluster.Annotation("tenant").Eq("t1")),
	AllowBookmarks: true,
})
if err != nil {
	return err
}

for event := range events {
	switch event.Type {
	case cluster.WatchAdded, cluster.WatchModified, cluster.WatchDeleted:
		_ = event.Object
		_ = event.Annotations
		_ = event.Changed
	case cluster.WatchBookmark:
		_ = event.ResourceVersion
	case cluster.WatchError:
		return event.Error
	}
}
```

常用 watch 参数：

| 参数 | 说明 |
| --- | --- |
| `Name` | 只 watch 指定对象名 |
| `Since` | 从指定 `resourceVersion` 之后开始 |
| `Selector` | 按 label、annotation、field 过滤 |
| `Scope` | watch 范围，支持 `WatchScopeObject`、`WatchScopeMetadata`、`WatchScopeStatus` |
| `AllowBookmarks` | 允许发送 bookmark 事件 |
| `SendInitialEvents` | 先把当前已有对象作为 `ADDED` 事件发出 |

只 watch 单个对象：

```go
events, err = widgets.Watch(ctx, cluster.WatchOptions{
	Name:  "alpha",
	Since: statused.Metadata.ResourceVersion,
})
```

只关注 metadata 或 status 变化：

```go
metadataEvents, err := widgets.WatchMetadata(ctx, cluster.WatchOptions{
	Name:  "alpha",
	Since: statused.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
_ = metadataEvents

statusEvents, err := widgets.WatchStatus(ctx, cluster.WatchOptions{
	Name:  "alpha",
	Since: statused.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
_ = statusEvents
```

也可以在 `Watch` 中显式设置 `Scope`：

```go
statusEvents, err = widgets.Watch(ctx, cluster.WatchOptions{
	Name:  "alpha",
	Since: statused.Metadata.ResourceVersion,
	Scope: cluster.WatchScopeStatus,
})
```

`WatchMetadata` 会返回 `metadata.labels`、`metadata.annotations`、
`metadata.finalizers`、`metadata.deletedAt` 等 metadata 变化事件。
`WatchStatus` 只返回 `status.<field>` 变化事件。`SendInitialEvents` 会先返回当前对象，
初始事件不受 `Scope` 过滤。

命名空间资源的 watch 范围跟句柄一致：`widgets.Namespace("team-a").Watch`
只返回 `team-a` 的事件；`widgets.Watch` 和 `widgets.AllNamespaces().Watch` 返回全部
namespace 的事件。事件对象里会带上 `Object.Metadata.Namespace`。

读取当前已有对象作为初始事件：

```go
events, err = widgets.Watch(ctx, cluster.WatchOptions{
	SendInitialEvents: true,
})
```

如果 `Since` 早于已清理的事件版本，watch 会返回 `WatchError`，错误为
`ErrResourceVersionTooOld`。

## 删除对象

没有 finalizer 时，`Delete` 会删除资源对象本身：

```go
deleted, err := widgets.Delete(ctx, "alpha", cluster.DeleteOptions{
	ResourceVersion: statused.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
_ = deleted
```

有 finalizer 时，第一次 `Delete` 只设置 `Metadata.DeletedAt`，对象仍然可以读取：

```go
withFinalizer, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{
	"finalizers": ["cleanup.example.test"]
}`), cluster.PatchOptions{})
if err != nil {
	return err
}

deleting, err := widgets.Delete(ctx, withFinalizer.Metadata.Name, cluster.DeleteOptions{
	ResourceVersion: withFinalizer.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
_ = deleting.Metadata.DeletedAt
```

清理完成后，先清空 finalizer，再次调用 `Delete` 删除对象本身：

```go
cleared, err := widgets.PatchMetadata(ctx, deleting.Metadata.Name, []byte(`{
	"finalizers": []
}`), cluster.PatchOptions{
	ResourceVersion: deleting.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = widgets.Delete(ctx, cleared.Metadata.Name, cluster.DeleteOptions{
	ResourceVersion: cleared.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
```

## 事件保留

watch 事件按 `event_retention_count` 保留最近的记录，默认 `2000`。旧事件只由当前
master 按 `event_cleanup_interval` 后台清理，清理间隔不得小于 `10ms`；普通资源写入不会触发全局清理。
从已清理版本开始的 watch 会收到 `ErrResourceVersionTooOld`。

## Node

每个 cluster 实例必须使用唯一的 node 名称启动。启动时会获取 node lease，同一
backend/prefix 下同名 node 不能同时存在。`Close` 会释放 lease，进程崩溃后需要等待
`node_lease_ttl` 过期后才能复用同名 node。

当前实例的 node 可以直接查询和修改：

```go
node, err := c.CurrentNode(ctx)
if err != nil {
	return err
}
_ = node.Metadata.Name

node, err = c.PatchCurrentNodeMetadata(ctx, []byte(`{
	"labels": {"role": "worker"},
	"annotations": {"zone": "test"}
}`), cluster.PatchOptions{})
if err != nil {
	return err
}

node, err = c.PatchCurrentNodeSpec(ctx, []byte(`{
	"metadata": {"owner": "platform"}
}`), cluster.PatchOptions{})
if err != nil {
	return err
}

node, err = c.UpdateCurrentNodeStatus(ctx, cluster.NodeStatus{
	Metadata: cluster.Annotations{"ready": "true"},
}, cluster.UpdateOptions{})
if err != nil {
	return err
}
```

也可以通过内置资源 `nodes` 做列表和 watch：

```go
nodes := c.Nodes()
nodeList, err := nodes.List(ctx, cluster.ListOptions{})
if err != nil {
	return err
}
_ = nodeList
```

## Master

每个 backend/prefix 下只有一个内置 master 对象：`masters/default`。cluster 启动后会
自动参与选举，并持续续约 master lease。当前 master 正常 `Close` 或调用 `StepDown`
时会释放 master；master lease 过期后，其他实例会尝试接管。

查询当前 master：

```go
master, err := c.Master(ctx)
if err != nil {
	return err
}
if master.Valid {
	_ = master.Node
	_ = master.Term
	_ = master.LeaseUntil
}

isMaster, err := c.IsMaster(ctx)
if err != nil {
	return err
}
_ = isMaster
```

监听 master 变化：

```go
events, err := c.WatchMaster(ctx, cluster.WatchOptions{
	Since: master.ResourceVersion,
})
if err != nil {
	return err
}

for event := range events {
	switch event.Type {
	case cluster.WatchModified:
		_ = event.Master
		_ = event.Transition
	case cluster.WatchError:
		return event.Error
	}
}
```

`WatchMaster` 只返回 master 节点或 term 变化，不会把普通续约事件暴露给调用方。
`Transition` 记录本次切换原因：

| 原因 | 说明 |
| --- | --- |
| `acquired` | 初次获得 master |
| `expired` | 接管已过期的 master lease |
| `released` | 当前 master 主动释放 |
| `lost` | 当前实例丢失 node lease |

读取最近切换记录：

```go
history, err := c.MasterHistory(ctx, 20)
if err != nil {
	return err
}
for _, transition := range history {
	_ = transition.From
	_ = transition.To
	_ = transition.Term
	_ = transition.At
}
```

当前实例主动让出 master：

```go
if err := c.StepDown(ctx); err != nil {
	if errors.Is(err, cluster.ErrNotMaster) {
		return nil
	}
	return err
}
```

`MasterHistory(ctx, 0)` 返回当前保留的全部记录。保留数量由 `master_history_limit`
控制，默认 `2000`。内置资源 `masters` 可以通过 `Resources` 查询，也可以通过
`c.Masters()` 读取或 watch。

## 资源信息

查询已注册资源和 schema：

```go
resources, err := c.Resources()
if err != nil {
	return err
}
for _, resource := range resources {
	_ = resource.Resource
	_ = resource.Builtin
	_ = resource.Namespaced
}

info, err := c.Resource("widgets")
if err != nil {
	return err
}
_ = info.Spec
_ = info.Status
_ = info.Annotations
```

内置 `nodes` 和 `masters` 也会出现在 `Resources` 返回值中。资源定义只支持查询，
不支持 watch。

## Unstructured

动态资源可以通过 `Unstructured` 句柄操作。资源仍然需要先 `Define`。

```go
raw, err := c.Unstructured("widgets")
if err != nil {
	return err
}

raw, err = raw.Namespace("team-a")
if err != nil {
	return err
}

obj, err := raw.Get(ctx, "alpha")
if err != nil {
	return err
}

patched, err := raw.Patch(ctx, obj.Metadata.Name, []byte(`{
	"spec": {"size": "medium"}
}`), cluster.PatchOptions{
	ResourceVersion: obj.Metadata.ResourceVersion,
})
if err != nil {
	return err
}
_ = patched
```

## 错误处理

```go
obj, err := widgets.Get(ctx, "alpha")
switch {
case err == nil:
	_ = obj
case errors.Is(err, cluster.ErrNotFound):
	return nil
case errors.Is(err, cluster.ErrConflict):
	return err
case errors.Is(err, cluster.ErrInvalidObject):
	return err
default:
	return err
}
```

常见错误：

| 错误 | 场景 |
| --- | --- |
| `ErrInvalidConfig` | URL 或配置无效 |
| `ErrInvalidResource` | 资源定义或资源名无效 |
| `ErrInvalidObject` | 对象名、字段、patch、schema 校验失败 |
| `ErrAlreadyExists` | 创建已存在对象 |
| `ErrNotFound` | 对象不存在 |
| `ErrConflict` | `resourceVersion` 冲突 |
| `ErrResourceVersionTooOld` | watch 起始版本早于已保留事件 |
| `ErrNodeAlreadyExists` | 同一 backend/prefix 下 node 名称已被占用 |
| `ErrNodeLeaseLost` | 当前实例丢失 node lease |
| `ErrNotMaster` | 当前实例不是有效 master |
| `ErrClosed` | cluster 已关闭 |

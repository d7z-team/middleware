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
- `badger:///path/to/db?node=worker-a&prefix=app`
- `etcd://127.0.0.1:2379?node=worker-a&prefix=app`

常用参数：

- `node`：当前实例的 node 名称，必填。
- `prefix`：badger / etcd 的存储前缀。
- `node_lease_ttl`：node lease TTL，默认 `30s`。
- `node_renew_interval`：node 续约间隔，默认 `10s`。
- `master_lease_ttl`：master lease TTL，默认跟随 `node_lease_ttl`。
- `master_renew_interval`：master 续约间隔，默认跟随 `node_renew_interval`。
- `master_history_limit`：保留的 master 切换记录数量，默认 `2000`。
- `event_retention_count`：保留的 watch 事件数量，默认 `2000`。
- `event_cleanup_interval`：master 清理旧 watch 事件的间隔。
- `watch_buffer_size`：每个 watch channel 的缓冲大小，默认 `256`。

## 定义资源

运行时资源定义以 JSON Schema 为准。常用方式是通过 Go struct 生成 schema，再用 `TypedResourceDef` 注册：

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type WidgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase"`
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

如果已经有完整 JSON Schema，也可以直接注册：

```go
raw, err := cluster.SchemaFrom[WidgetSpec, WidgetStatus]("example.test/v1", "Widget", false)
if err != nil {
	return err
}

_, err = cluster.DefineResource(c, cluster.ResourceDef{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
	Schema:     raw,
})
if err != nil {
	return err
}
```

`cluster` tag 当前用于 schema 生成：

- `required`
- `enum=a|b|c`
- `immutable`
- `index`
- `default=value`
- `nullable`
- `preserveUnknown`

## 命名空间

资源默认是 cluster-scoped。定义时设置 `Namespaced: true` 后，必须先绑定 namespace 句柄：

```go
jobs, err := cluster.Define(c, cluster.TypedResourceDef[JobSpec, JobStatus]{
	Resource:   "jobs",
	APIVersion: "example.test/v1",
	Kind:       "Job",
	Namespaced: true,
})
if err != nil {
	return err
}

billing, err := jobs.Namespace("billing")
if err != nil {
	return err
}
```

`AllNamespaces()` 只用于 `List` 和 `Watch`。

## CRUD

创建、读取、更新、状态更新、删除：

```go
created, err := widgets.Create(ctx, "alpha", WidgetSpec{
	Owner: "team-a",
}, cluster.CreateOptions{})
if err != nil {
	return err
}

got, err := widgets.Get(ctx, "alpha")
if err != nil {
	return err
}

patched, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), cluster.PatchOptions{
	ResourceVersion: got.Metadata.ResourceVersion,
})
if err != nil {
	return err
}

_, err = widgets.UpdateStatus(ctx, "alpha", WidgetStatus{
	Phase: "Ready",
}, cluster.UpdateOptions{
	ResourceVersion: patched.Metadata.ResourceVersion,
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

- 主资源 `Create/Update/Patch` 不能修改 status。
- `UpdateStatus/PatchStatus` 只修改 status。
- `PatchMetadata` 只修改 `labels`、`annotations`、`finalizers`。
- `resourceVersion` 作为 CAS 条件。
- 有 finalizer 的对象第一次删除只会写入 `deletedAt`。

## List 与 Selector

```go
list, err := widgets.List(ctx, cluster.ListOptions{
	Selector: cluster.Where(
		cluster.Field("status.phase").Eq("Ready"),
		cluster.Field("spec.owner").Eq("team-a"),
		cluster.Label("app").Eq("demo"),
	),
})
if err != nil {
	return err
}
```

当前 field selector 规则：

- 默认允许 `metadata.name`。
- namespaced 资源额外允许 `metadata.namespace`。
- `spec.*` 和 `status.*` 必须在 schema 中声明 `x-cluster-index`，或通过 `cluster:"index"` 生成。
- 未声明索引的字段会直接返回 `ErrInvalidObject`。

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

for event := range events {
	_ = event
}
```

可用接口：

- `Watch`
- `WatchMetadata`
- `WatchStatus`

事件类型：

- `ADDED`
- `MODIFIED`
- `DELETED`
- `BOOKMARK`
- `ERROR`

## 资源发现

```go
info, err := c.Resource("widgets")
if err != nil {
	return err
}

_ = info.Schema
_ = info.Indexes
_ = info.Admission
```

`ResourceInfo` 返回：

- `Resource`
- `APIVersion`
- `Kind`
- `Namespaced`
- `Schema`
- `Indexes`
- `Admission`
- `Builtin`

## Node 与 Master

内置资源：

- `nodes`
- `masters`

当前实例 node：

```go
node, err := c.CurrentNode(ctx)
if err != nil {
	return err
}

_, err = c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"role":"worker"}}`), cluster.PatchOptions{})
if err != nil {
	return err
}
```

master 查询：

```go
master, err := c.Master(ctx)
if err != nil {
	return err
}

isMaster, err := c.IsMaster(ctx)
if err != nil {
	return err
}

history, err := c.MasterHistory(ctx, 10)
if err != nil {
	return err
}
```

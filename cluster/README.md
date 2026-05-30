# cluster 使用说明

## 创建 Cluster

```go
c, err := cluster.NewClusterFromURL("memory://?watch_buffer_size=256")
if err != nil {
	return err
}
defer c.Close()
```

可用 URL：

| URL | 用途 |
| --- | --- |
| `memory://` | 进程内存储 |
| `mem://` | `memory://` 的简写 |
| `badger:///path/to/db?prefix=app` | 本地持久化存储 |
| `etcd://127.0.0.1:2379?prefix=app` | etcd 存储 |

可用参数：

| 参数 | 说明 |
| --- | --- |
| `prefix` | 存储前缀 |
| `event_retention_count` | 保留的 watch 事件数量，默认 `2000`，传入时必须大于 `0` |
| `watch_buffer_size` | 每个 watch channel 的缓冲大小，默认 `256` |

## 定义资源

每类资源需要先调用 `Define`。`S` 是 `spec` 类型，`T` 是 `status` 类型。

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,watch"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type WidgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase,watch"`
}

widgets, err := cluster.Define(c, cluster.ResourceDef[WidgetSpec, WidgetStatus]{
	Resource:   "widgets",
	APIVersion: "example.test/v1",
	Kind:       "Widget",
	Annotations: []cluster.AnnotationRule{
		{Key: "tenant", Required: true, Immutable: true, Indexed: true, Watch: true},
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
| `Annotations` | metadata annotation 规则 |
| `Default` | 创建或更新 spec 时填默认值 |
| `Validate` | 创建、更新 spec、更新 status 时做校验 |

资源名不能是空值、`.`、`..`，也不能包含 `/` 或 `\`。

## 字段规则

字段规则写在 `spec` 或 `status` 的导出字段 tag 上：

```go
type WidgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,watch"`
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
| `watch` | 声明字段用于 watch 关注 |

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
	{Key: "tenant", Required: true, Immutable: true, Indexed: true, Watch: true},
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
| `Watch` | 声明注解用于 watch 关注 |

修改 metadata 注解使用 `Patch`：

```go
patched, err := widgets.Patch(ctx, created.Metadata.Name, []byte(`{
	"metadata": {
		"annotations": {
			"owner": "platform"
		}
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
`metadata.finalizers` 和 `spec`。`status` 需要使用 status 方法修改。

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
		cluster.Label("app").Eq("demo"),
		cluster.Annotation("tenant").Eq("t1"),
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
| `cluster.Annotation("tenant").Eq("t1")` | 匹配 annotation |
| `cluster.Field("metadata.name").Eq("alpha")` | 匹配 metadata 字段 |
| `cluster.Field("spec.size").Eq("large")` | 匹配 spec 字段 |
| `cluster.Field("status.phase").Eq("Ready")` | 匹配 status 字段 |

字段 selector 支持：

- `metadata.name`
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

读取当前已有对象作为初始事件：

```go
events, err = widgets.Watch(ctx, cluster.WatchOptions{
	SendInitialEvents: true,
})
```

如果 `Since` 早于已压缩的事件版本，watch 会返回 `WatchError`，错误为
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
withFinalizer, err := widgets.Patch(ctx, "alpha", []byte(`{
	"metadata": {"finalizers": ["cleanup.example.test"]}
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
cleared, err := widgets.Patch(ctx, deleting.Metadata.Name, []byte(`{
	"metadata": {"finalizers": []}
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

## 压缩事件

```go
if err := c.Compact(ctx, list.ResourceVersion); err != nil {
	return err
}
```

压缩后，从更早版本开始的 watch 会收到 `ErrResourceVersionTooOld`。

## Unstructured

动态资源可以通过 `Unstructured` handle 操作。资源仍然需要先 `Define`。

```go
raw, err := c.Unstructured("widgets")
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
| `ErrClosed` | cluster 已关闭 |

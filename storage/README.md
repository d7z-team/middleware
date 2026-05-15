# storage

`storage` 提供了一组基于 `afero` 的统一文件系统抽象，当前支持：

- `memory://` / `mem://`
- `local:///abs/path` / `storage:///abs/path`
- `overlay://` / `mount://`
- `s3://bucket/root/prefix`

S3 后端的目标不是把对象存储伪装成完整 POSIX 文件系统，而是在保持 `storage.Storage` 统一接口的前提下，尽量提供可用的文件式访问语义。

## S3 URL

示例：

```text
s3://bucket/root/prefix?region=us-east-1&endpoint=http://127.0.0.1:9000&path_style=true&access_key=minioadmin&secret_key=minioadmin&disable_ssl=true
```

常用参数：

- `region`
- `endpoint`
- `path_style`
- `access_key`
- `secret_key`
- `session_token`
- `profile`
- `disable_ssl`
- `temp_dir`

说明：

- `host` 是 bucket 名称。
- `path` 是 storage root prefix。
- bucket 必须预先存在，当前实现不会自动创建 bucket。
- `temp_dir` 用于本地临时文件缓冲，默认是系统临时目录。

## 当前支持

S3 后端当前支持这些基础能力：

- `Create`
- `Open`
- `OpenFile`
- `Mkdir`
- `MkdirAll`
- `Stat`
- `Remove`
- `RemoveAll`
- `Rename`
- `Child`

目录通过两种方式识别：

- 显式目录 marker object，例如 `dir/`
- 带子对象的前缀，例如 `dir/file.txt`

## 缺失功能与弱语义

下面这些点是当前 S3 后端和本地文件系统最重要的差异，也是调用方最需要知道的部分。

### 1. 不是完整 POSIX 文件系统

下面这些能力当前没有实现：

- symlink / readlink
- hard link
- `Lstat`
- 文件锁
- 文件系统事件通知
- 特殊文件类型，例如 socket、device、pipe

这不是简单的“还没补”，而是对象存储本身就不天然提供这类语义。

### 2. 权限和时间戳不是实际生效的元数据

`Chmod`、`Chown`、`Chtimes` 当前只做“路径是否存在”的检查，然后返回成功。

也就是说：

- 不会把 POSIX 权限写回 S3
- 不会保存 owner/group
- 不会真正修改对象时间戳

这样做是为了兼容上层 `mountfs` 的跨后端复制/重命名流程，而不是为了提供真实的 POSIX 元数据支持。

### 3. 写入不是原地修改，而是整对象重写

当前实现对文件写入使用“本地临时文件 + 完整上传”的模式：

1. 打开已有对象时，先下载完整对象到本地临时文件
2. `Write` / `Append` / `Truncate` 都作用在本地临时文件上
3. `Sync` 或 `Close` 时，再把整个对象重新上传到 S3

这意味着：

- 不支持服务端原地修改
- 不支持真正的随机块更新
- `Append` 只是模拟语义，不是 S3 原生追加
- 大文件会占用本地临时磁盘空间
- 写入成本和对象总大小强相关，而不是和变更量强相关

因此它更适合配置、静态资源、中小型对象，不适合高频增量写场景。

### 4. `Rename` 不是原子操作

S3 没有原生 rename，当前实现是：

- 文件：`copy + delete`
- 目录：递归 `copy + delete`

这带来几个后果：

- `Rename` 不是原子的
- 目录重命名中途失败时，目标前缀下可能已经复制出一部分对象
- 并发读写同一路径时，外部观察到的状态可能是过渡态

如果调用方需要事务式移动，这个后端不提供保证。

### 5. 并发写同一对象没有冲突检测

当前实现没有基于 ETag、VersionID 或条件请求做乐观锁控制。

结果是：

- 多个 writer 同时修改同一个 key 时，最后一次成功上传会覆盖前面的结果
- 不会自动报“写冲突”
- 不会自动做 merge

如果业务需要并发一致性控制，必须在上层自己做。

### 6. 目录语义是模拟出来的

S3 没有真实目录，当前目录是通过 marker object 和 prefix 规则模拟的。

这意味着：

- 空目录只有在通过当前实现显式 `Mkdir` / `MkdirAll` 创建后才稳定可见
- 如果外部工具直接往 bucket 写对象，目录结构能否完全符合本地文件系统直觉，取决于对象 key 布局
- 如果外部数据同时制造了“同名文件”和“同名前缀目录”的冲突结构，行为不会像本地文件系统那样自然

当前实现内部会尽量避免自己制造这类冲突，但不能阻止外部系统往同一 bucket/prefix 写出异常结构。

### 7. `Remove` 和 `RemoveAll` 的语义不同

- `Remove` 删除非空目录会返回错误
- `RemoveAll` 才会递归删除整个前缀

这是刻意保留的防误删语义，不会把 `Remove` 做成隐式递归删除。

### 8. bucket 生命周期和安全策略不在这个后端里

当前实现不会管理这些资源层面的能力：

- bucket 创建
- bucket policy
- lifecycle rule
- versioning
- object lock
- encryption policy
- replication

这些都需要由基础设施层或外部初始化脚本负责。

## 适合与不适合的场景

更适合：

- 配置文件
- 归档类文件
- 静态资源
- 中小体积、低并发修改的对象

不适合：

- 高频小块增量更新
- 需要原子 rename 的流程
- 强依赖 POSIX 权限/owner/timestamp 的程序
- 多 writer 并发覆盖同一路径且要求冲突检测的场景


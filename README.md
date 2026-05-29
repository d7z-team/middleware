# Golang Middleware

[![Go Report Card](https://goreportcard.com/badge/gopkg.d7z.net/middleware)](https://goreportcard.com/report/gopkg.d7z.net/middleware) 
[![Coverage](https://codecov.io/gh/d7z-team/middleware/graph/badge.svg?token=VNT15HBWN0)](https://codecov.io/gh/d7z-team/middleware)
[![Tests](https://github.com/d7z-team/middleware/actions/workflows/go-test.yml/badge.svg)](https://github.com/d7z-team/middleware/actions/workflows/go-test.yml)
[![Godoc](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godocs.io/gopkg.d7z.net/middleware)
[![LICENSE](https://img.shields.io/github/license/d7z-team/middleware.svg?style=flat-square)](https://github.com/d7z-team/middleware/blob/main/LICENSE)

> 平台自用的中间件

请勿用于 **任何** 生产环境

## Packages

- `cache`: memory / Redis blob cache with metadata and TTL.
- `kv`: hierarchical key-value abstraction with TTL, scan, batch operations, CAS, and conditional atomic updates.
- `lock`: memory / etcd locking primitives.
- `queue`: memory / etcd at-least-once queue.
- `storage`: afero-based memory, local, overlay, and S3 storage.
- `subscribe`: memory / etcd pub/sub primitives.
- `cluster`: lightweight KV-only cluster coordination with Raft-style leader election, leader lease fencing, and optional KV task claims.
- `tools`: typed helpers built on top of `kv` and `cache`.

## License

Apache-2.0

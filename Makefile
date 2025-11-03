GOPATH := $(shell go env GOPATH)
GOARCH ?= $(shell go env GOARCH)

.PHONY: deps
deps: minio etcd

.PHONY: minio
minio:
	@docker kill sync-minio ||: && docker run -d --rm --name sync-minio --network host \
	--env MINIO_ROOT_USER="minio_admin" \
  --env MINIO_ROOT_PASSWORD="minio_admin" \
	quay.io/minio/minio server /data --console-address ":9001"

.PHONY: etcd
etcd:
	@docker kill sync-etcd ||: && docker run -d --rm --name sync-etcd --network host docker.io/rancher/etcd:v3.4.13-k3s1 etcd

.PHONY: fmt
fmt:
	@(test -f "$(GOPATH)/bin/gofumpt" || go install mvdan.cc/gofumpt@latest) && \
	"$(GOPATH)/bin/gofumpt" -l -w .

.PHONY: lint
lint:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
	"$(GOPATH)/bin/golangci-lint" run -c .golangci.yml

.PHONY: lint-fix
lint-fix:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
	"$(GOPATH)/bin/golangci-lint" run -c .golangci.yml --fix

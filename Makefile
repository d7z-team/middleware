GOPATH := $(shell go env GOPATH)
GOARCH ?= $(shell go env GOARCH)

.PHONY: deps
deps: etcd redis minio

.PHONY: redis
redis:
	@docker kill sync-redis ||:  && sleep 1&& docker run -d --rm --name sync-redis --network host docker.io/library/redis:alpine

.PHONY: etcd
etcd:
	@docker kill sync-etcd ||:  && sleep 1 && docker run -d --rm --name sync-etcd --network host docker.io/rancher/etcd:v3.4.13-k3s1 etcd

.PHONY: minio
minio:
	@docker kill sync-minio ||: && sleep 1 && docker run -d --rm --name sync-minio --network host \
		-e MINIO_ROOT_USER=minioadmin \
		-e MINIO_ROOT_PASSWORD=minioadmin \
		docker.io/minio/minio server /data --address :9000 --console-address :9001
	@until curl -fsS http://127.0.0.1:9000/minio/health/ready >/dev/null; do sleep 1; done

.PHONY: minio-stop
minio-stop:
	@docker kill sync-minio ||:

.PHONY: fmt
fmt:
	@(test -f "$(GOPATH)/bin/gofumpt" || go install mvdan.cc/gofumpt@latest) && \
	"$(GOPATH)/bin/gofumpt" -l -w . && go mod tidy

.PHONY: lint
lint:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
	"$(GOPATH)/bin/golangci-lint" run -c .golangci.yml

.PHONY: lint-fix
lint-fix:
	@(test -f "$(GOPATH)/bin/golangci-lint" || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.6.0) && \
	"$(GOPATH)/bin/golangci-lint" run -c .golangci.yml --fix


.PHONY: test
test:
	@go test  -v -coverprofile=coverage.txt -timeout=60s ./...

.PHONY: test-storage
test-storage: minio
	@go test -v ./storage

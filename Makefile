GOPATH := $(shell go env GOPATH)
GOARCH ?= $(shell go env GOARCH)

.PHONY: deps
deps: etcd redis

.PHONY: redis
redis:
	@docker kill sync-redis ||:  && sleep 1&& docker run -d --rm --name sync-redis --network host docker.io/library/redis:alpine

.PHONY: etcd
etcd:
	@docker kill sync-etcd ||:  && sleep 1 && docker run -d --rm --name sync-etcd --network host docker.io/rancher/etcd:v3.4.13-k3s1 etcd

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


.PHONY: test
test:
	@go test -v -coverprofile=coverage.txt ./...
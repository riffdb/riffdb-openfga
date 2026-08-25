GO ?= go
GO_PROXY ?= http://127.0.0.1:4874/go,https://proxy.golang.org,direct
GO_NO_SUMDB ?= riffdb.dev/*

.PHONY: bootstrap check fmt test vet conformance

bootstrap:
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) mod download

fmt:
	$(GO) fmt ./...

test:
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) test ./...

vet:
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) vet ./...

check: fmt vet test
	git diff --exit-code

conformance:
	@echo "conformance harness is delivered by WP-006" >&2
	@exit 1

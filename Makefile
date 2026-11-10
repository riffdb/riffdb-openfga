GO ?= go
RIFFDB ?= $(shell if [ -x "$(CURDIR)/../riffdb/target/release/riffdb" ]; then echo "$(CURDIR)/../riffdb/target/release/riffdb"; else echo riffdb; fi)
RIFFDB_VERSION ?= v0.2.1
LOCAL_GO_PROXY := $(CURDIR)/.cache/go-proxy
GO_PROXY ?= file://$(LOCAL_GO_PROXY),https://proxy.golang.org,direct
GO_NO_SUMDB ?= riffdb.dev/*

.PHONY: bootstrap build run-dev check fmt test vet conformance authority

bootstrap:
	mkdir -p "$(LOCAL_GO_PROXY)/riffdb.dev/application/@v"
	for ext in info mod zip; do \
		target="$(LOCAL_GO_PROXY)/riffdb.dev/application/@v/$(RIFFDB_VERSION).$$ext"; \
		if [ ! -s "$$target" ]; then \
			curl -fsSL "https://github.com/riffdb/riffdb/releases/download/$(RIFFDB_VERSION)/$(RIFFDB_VERSION).$$ext" -o "$$target"; \
		fi; \
	done
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) mod download

build:
	CGO_ENABLED=0 GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) \
		$(GO) build -trimpath -ldflags="-s -w" -o openfga-riffdb ./cmd/openfga-riffdb

run-dev:
	CARGO_BUILD_RUSTC_WRAPPER= RUSTC_WRAPPER= \
		GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) \
		$(RIFFDB) dev --run --go-runner-package cmd/openfga-riffdb

fmt:
	$(GO) fmt . ./cmd/... ./internal/...

test:
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) test ./...

vet:
	GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) $(GO) vet ./...

authority:
	./scripts/check-role-authority

check: fmt vet test authority
	git diff --exit-code

conformance:
	CARGO_BUILD_RUSTC_WRAPPER= RUSTC_WRAPPER= \
		GONOSUMDB=$(GO_NO_SUMDB) GOPROXY=$(GO_PROXY) \
		$(RIFFDB) dev --run --go-runner-package cmd/conformance

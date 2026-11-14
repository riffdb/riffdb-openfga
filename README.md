# RiffDB OpenFGA datastore

A standalone Go implementation of OpenFGA's datastore interface backed by a
compiled RiffDB application.

Status: v0.3.5. The complete pinned OpenFGA datastore interface is implemented
and the official `storage/test.RunAllTests` suite passes against a real RiffDB
loopback service. Prebuilt `openfga-riffdb` standalone server binaries and a
multi-stage container image (`Dockerfile`) are included.

## Boundary

OpenFGA-specific contracts, generated bindings, protobuf mapping, error
mapping, and conformance tests live here. RiffDB remains application-neutral.
Every write dispatches a generated compiled command; every read dispatches a
generated named query. The adapter does not expose SQL, generic
CRUD, dynamic query text, or transaction callbacks.

The initial compatibility target is OpenFGA `v1.18.3`. OpenFGA's storage
interface is an internal server integration seam that can change between
releases, so upgrades are deliberate and tested rather than expressed as a
broad version range.

## Use

### 1. Prebuilt CLI & Docker Runner (`cmd/openfga-riffdb`)

Because upstream OpenFGA's `cmd/run` hardcodes its datastore switch (`memory`,
`postgres`, `mysql`, `sqlite`), this repository ships a drop-in OpenFGA server
binary (`cmd/openfga-riffdb`) and multi-stage `Dockerfile` that preserve the
full stock OpenFGA CLI and runtime surface while persisting to RiffDB:

- All ~70 stock `openfga run` flags, `OPENFGA_*` environment variables, and
  `config.yaml` configuration (`/etc/openfga`, `$HOME/.openfga`, `.`).
- Built-in authentication (`--authn-method=none|preshared|oidc`, including OIDC
  discovery, JWKS rotation, issuer aliases, audience/subject validation, and
  custom client ID claims).
- Prometheus `/metrics` (`--metrics-enabled`, `--metrics-addr`,
  `--metrics-enable-rpc-histograms`).
- Embedded OpenFGA Playground (`--playground-enabled`, `--playground-addr`).
- HTTP/REST `grpc-gateway` with CORS, `/healthz`, AuthZEN, and TLS certificate
  hot-reloading (`--grpc-tls-*`, `--http-tls-*`).
- OpenTelemetry OTLP trace exporting (`--trace-enabled`) and `pprof` profiler
  (`--profiler-enabled`).
- `openfga-riffdb validate-models` and `openfga-riffdb version` subcommands.
- Direct compatibility with `riffdb dev --run --go-runner-package cmd/openfga-riffdb`
  (`make run-dev`).

Build and run the standalone server binary or container:

```bash
# Build stripped binary (~32 MB, no SQL drivers or k8s client bloat)
make build
./openfga-riffdb run \
  --riffdb-socket /run/riffdb/driver.sock \
  --riffdb-remote-identity-hash <remote-sha256> \
  --authn-method preshared \
  --authn-preshared-keys secret-key-1

# Or build the distroless container image (/openfga-riffdb and /openfga symlink)
docker build -t openfga-riffdb .

# Or launch against a local RiffDB development instance in one command
make run-dev
```

### 2. Programmatic Go Embedding

When embedding in your own Go binary, use `riffdbopenfga.Connect` (or
`riffdbopenfga.DefaultIdentity`) so you do not need to manually copy the 6
compile-time SHA-256 hashes from `riffdb.application.lock.json`:

```go
session, err := riffdbopenfga.Connect(ctx, riffdbopenfga.ConnectConfig{
    SocketPath:         "/run/riffdb/driver.sock",
    Database:           "openfga-production",
    RemoteIdentityHash: remoteHash,
})
if err != nil {
    return err
}

datastore, err := riffdbopenfga.NewDatastore(riffdbopenfga.Config{
    Session:         session,
    CommandAttempts: 3,
    CloseSession:    true,
})
```

`Datastore` implements
`github.com/openfga/openfga/pkg/storage.OpenFGADatastore` and can be supplied
to an embedded OpenFGA server with `server.WithDatastore`.

The caller owns the session unless `CloseSession` is selected. Command retries
are bounded to 1 through 10 attempts. Public OpenFGA page sizes are accepted,
while each RiffDB query uses a finite internal page bound.

## Local development

RiffDB's Go runtime is pinned to the `v0.2.5` release. With the matching RiffDB
CLI available as `riffdb`:

```bash
make bootstrap
make check
make conformance
```

`make bootstrap` fetches `riffdb.dev/application@v0.2.5` from the signed RiffDB
GitHub release into `.cache/go-proxy` and resolves public dependencies through
the public Go proxy. `make conformance` launches the compiled application and runs the
official OpenFGA datastore suite through the real Go driver transport. No
global Go configuration is modified.

See [DESIGN.md](./DESIGN.md) for the semantic inventory and
[PLAN.md](./PLAN.md) for the implementation sequence and stop gates.


## RiffDB & OpenFGA compatibility CI

This adapter owns its RiffDB and upstream OpenFGA compatibility checks:

- **RiffDB compatibility (`.github/workflows/riffdb-compatibility.yml`)**: Pull
  requests compile the application sources and verify existing locks and
  generated artifacts against the full commit in `.github/riffdb-revision`.
  Scheduled checks use RiffDB `main`; manual dispatch accepts a full commit or
  `main`.
- **OpenFGA compatibility (`.github/workflows/openfga-compatibility.yml`)**:
  Weekly scheduled checks and manual dispatch resolve the latest upstream
  `openfga/openfga` release tag and verify that the datastore surface, CLI
  runner, and unit suite compile and pass against it.

To upgrade RiffDB, update the pin, migrate sources as needed, review the result
of `riffdb application lock --write`, and verify with `riffdb application check`.
Commit the pin and migration together in this repository.

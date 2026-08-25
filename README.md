# RiffDB OpenFGA datastore

A standalone Go implementation of OpenFGA's datastore interface backed by a
compiled RiffDB application.

Status: repository bootstrap; not yet usable as an OpenFGA datastore. The
upstream interface and development dependencies are pinned, and the initial
capability/conformance plan is checked in. No incomplete datastore constructor
is exported.

## Boundary

OpenFGA-specific contracts, generated bindings, protobuf mapping, error
mapping, and conformance tests live here. RiffDB remains application-neutral.
Every write will dispatch a generated compiled command; every read will
dispatch a generated named query. The adapter will not expose SQL, generic
CRUD, dynamic query text, or transaction callbacks.

The initial compatibility target is OpenFGA `v1.18.3`. OpenFGA's storage
interface is an internal server integration seam that can change between
releases, so upgrades are deliberate and tested rather than expressed as a
broad version range.

## Intended use

The package will implement:

```go
github.com/openfga/openfga/pkg/storage.OpenFGADatastore
```

for embedding with `server.WithDatastore`. OpenFGA's stock CLI has a closed
set of datastore-engine flags, so this repository will also provide a small
launcher once the datastore is complete. It will not claim that an unmodified
`openfga run --datastore-engine riffdb` command exists.

## Local development

RiffDB's Go runtime is pinned to the immutable local development publication
`v0.1.0-dev.14`. With the loopback registry running:

```bash
make bootstrap
make check
```

`make bootstrap` resolves `riffdb.dev/application` through
`http://127.0.0.1:4874/go` and resolves public dependencies through the public
Go proxy. No global Go configuration is modified.

See [DESIGN.md](./DESIGN.md) for the semantic inventory and
[PLAN.md](./PLAN.md) for the implementation sequence and stop gates.


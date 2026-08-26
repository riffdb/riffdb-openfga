# RiffDB OpenFGA datastore

A standalone Go implementation of OpenFGA's datastore interface backed by a
compiled RiffDB application.

Status: the complete pinned OpenFGA datastore interface is implemented. The
official `storage/test.RunAllTests` suite passes against a real RiffDB
loopback service. The module remains an unpublished development package.

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

Construct the datastore with an authenticated session for this compiled
application:

```go
datastore, err := riffdbopenfga.NewDatastore(riffdbopenfga.Config{
    Session:         session,
    CommandAttempts: 3,
})
```

`Datastore` implements
`github.com/openfga/openfga/pkg/storage.OpenFGADatastore` and can be supplied
to an embedded OpenFGA server with `server.WithDatastore`. OpenFGA's stock CLI
has a closed set of datastore-engine flags; an unmodified
`openfga run --datastore-engine riffdb` command does not exist.

The caller owns the session unless `CloseSession` is selected. Command retries
are bounded to 1 through 10 attempts. Public OpenFGA page sizes are accepted,
while each RiffDB query uses a finite internal page bound.

## Local development

RiffDB's Go runtime is pinned to the immutable local development publication
`v0.1.0-dev.18`. With the local registries running and the matching RiffDB CLI
available as `riffdb`:

```bash
make bootstrap
make check
make conformance
```

`make bootstrap` resolves `riffdb.dev/application` through
`http://127.0.0.1:4874/go` and resolves public dependencies through the public
Go proxy. `make conformance` launches the compiled application and runs the
official OpenFGA datastore suite through the real Go driver transport. No
global Go configuration is modified.

See [DESIGN.md](./DESIGN.md) for the semantic inventory and
[PLAN.md](./PLAN.md) for the implementation sequence and stop gates.

# AGENTS.md — RiffDB OpenFGA adapter

## Mission

Implement a standalone Go adapter that satisfies OpenFGA's pinned
`storage.OpenFGADatastore` contract using a compiled RiffDB application. All
OpenFGA-specific behavior belongs in this repository, never in RiffDB.

## Authoritative inputs

- OpenFGA `v1.18.3` and its exported `pkg/storage/test.RunAllTests` suite.
- This repository's `DESIGN.md` and `PLAN.md`.
- The accepted RiffDB specification and ADRs at the RiffDB revision recorded in
  `go.mod` and the generated application lock.

If these inputs conflict, stop and request maintainer review. Do not weaken an
OpenFGA semantic or a RiffDB safety guarantee to make a test pass.

## Architecture boundaries

1. OpenFGA writes use generated, compiled RiffDB commands only.
2. Reads use generated named RiffQL queries only. Do not add generic CRUD,
   dynamically assembled predicates, or client-side full-store scans.
3. One OpenFGA tuple write call atomically applies deletes before inserts and
   persists its changelog entries.
4. Pagination is bounded and continuation tokens are validated, opaque, and
   tied to the operation and filter that created them.
5. The caller owns the RiffDB driver session. The adapter must not weaken
   transport identity, authorization, retry, or read-after-commit behavior.
6. Protobuf values stored as bytes use deterministic serialization and strict,
   size-bounded decoding.
7. Never emulate unsupported semantics with an unbounded read, process-local
   correctness state, wall-clock sleeps, or a silent fallback.
8. Public errors map deliberately to OpenFGA storage errors and do not expose
   RiffDB internals, credentials, or stored authorization data.

## Stop conditions

Pause for a RiffDB capability or DX review if implementation would require:

- non-atomic tuple mutation and changelog persistence;
- process-local ordering to define the durable OpenFGA change sequence;
- an unbounded query or client-side authorization filtering;
- arbitrary transaction callbacks or arbitrary query text;
- target-language reimplementation of RiffDB transport guarantees; or
- OpenFGA-specific behavior in the RiffDB repository.

## Completion checks

Before calling the adapter usable:

```bash
make check
make conformance
```

The conformance target must run OpenFGA's exported datastore suite against a
real RiffDB service and driver host. Unit fakes may supplement it but never
replace it.


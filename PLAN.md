# Implementation plan

## WP-001 — Repository and upstream pin

Status: complete.

- Pin OpenFGA `v1.18.3` and RiffDB Go runtime `v0.2.1`.
- Lock the 21-method upstream interface with a reflection-based surface test.
- Establish the package boundary, local registry workflow, and stop gates.

Exit: the compatibility surface and dependency workflow are locked.

## WP-002 — Contract and generated boundary

Status: complete.

- Write the complete bounded RiffDB contract and named query catalog.
- Add the application manifest, lock, deployment configuration, and generated
  Go facade.
- Prove each query uses a covering index for all predicates before pagination.
- Add artifact-freshness checks and generated-boundary tests.

Exit: generated artifacts are exact and every accepted query shape is backed
by a bounded named query and covering index.

## WP-003 — Stores, models, and assertions

Status: complete.

- Implement protobuf codecs and size bounds.
- Implement store lifecycle, model history/latest lookup, and assertion
  replacement.
- Map public errors and cancellation.
- Run the corresponding official OpenFGA test groups against real RiffDB.

## WP-004 — Tuple writes and changelog

Status: complete.

- Implement one atomic delete-before-insert command with duplicate/missing
  policies and matching changelog rows.
- Prove retry idempotency and concurrent conflict mapping.
- Implement change direction, object-type, horizon, and ULID continuation.
- Exercise retries and concurrency through the official datastore suite and
  real RiffDB driver transport.

## WP-005 — Tuple query matrix and iterators

Status: complete.

- Implement exact, partial, userset, and reverse reads.
- Implement bounded lazy iterators and paged reads.
- Prove conditioned/unconditioned matching, case sensitivity, sorted reverse
  reads, filter-bound cursors, cancellation, and resource release.

## WP-006 — Full OpenFGA conformance and launcher

Status: complete.

- Run `storage/test.RunAllTests` against a real RiffDB service.
- Provide a repeatable real-driver conformance runner (`cmd/conformance`).
- Export locked application identity constants and `Connect` helpers (`identity.go`).
- Provide a standalone CLI and container runner (`cmd/openfga-riffdb` and `Dockerfile`)
  supporting the stock OpenFGA CLI flags, `OPENFGA_*` environment variables,
  `config.yaml`, authentication (`none`, `preshared`, `oidc`), Prometheus
  `/metrics`, OpenFGA Playground, HTTP/REST `grpc-gateway`, TLS certificate
  hot-reloading, OTLP tracing, `pprof`, and `riffdb dev --run`.
- Document supported configuration and explicit limits.

## WP-007 — Release readiness

Status: complete.

- Add compatibility policy, package metadata, RiffDB compatibility CI, upstream
  OpenFGA compatibility CI, prebuilt `openfga-riffdb` release binary packaging,
  and release provenance.
- Verify both `make check` and `make conformance` gates are green.

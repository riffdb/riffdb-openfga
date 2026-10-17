# Implementation plan

## WP-001 — Repository and upstream pin

Status: complete.

- Pin OpenFGA `v1.18.3` and RiffDB Go runtime `v0.1.0-dev.19`.
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

Status: complete for the datastore package and conformance harness.

- Run `storage/test.RunAllTests` against a real RiffDB service.
- Provide a repeatable real-driver conformance runner.
- Document supported configuration and explicit limits.

The deployable OpenFGA process that supplies the authenticated RiffDB session
is intentionally an application concern. The adapter exports the standard
`OpenFGADatastore` implementation needed by `server.WithDatastore`; it does not
claim a stock OpenFGA datastore-engine flag.

## WP-007 — Release readiness

Status: deferred; publishing is out of scope for now.

- Add compatibility policy, changelog, package metadata, CI, and release
  provenance.
- Do not publish until the conformance and loopback gates are green.

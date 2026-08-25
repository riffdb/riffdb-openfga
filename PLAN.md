# Implementation plan

## WP-001 — Repository and upstream pin

Status: complete.

- Pin OpenFGA `v1.18.3` and RiffDB Go runtime `v0.1.0-dev.14`.
- Lock the 21-method upstream interface with a reflection-based surface test.
- Establish the package boundary, local registry workflow, and stop gates.

Exit: `make check` passes without exporting an incomplete datastore.

## WP-002 — Contract and generated boundary

Status: next.

- Write the complete bounded RiffDB contract and named query catalog.
- Add the application manifest, lock, deployment configuration, and generated
  Go facade.
- Prove each query uses a covering index for all predicates before pagination.
- Add artifact-freshness checks and generated-boundary tests.

Stop gate: review change ordering before accepting the tuple/changelog schema.

## WP-003 — Stores, models, and assertions

Status: pending.

- Implement protobuf codecs and size bounds.
- Implement store lifecycle, model history/latest lookup, and assertion
  replacement.
- Map public errors and cancellation.
- Run the corresponding official OpenFGA test groups against real RiffDB.

## WP-004 — Tuple writes and changelog

Status: pending.

- Implement one atomic delete-before-insert command with duplicate/missing
  policies and matching changelog rows.
- Prove retry idempotency and concurrent conflict mapping.
- Implement change direction, object-type, horizon, and ULID continuation.
- Add process-level retry and multi-client concurrency tests.

## WP-005 — Tuple query matrix and iterators

Status: pending.

- Implement exact, partial, userset, and reverse reads.
- Implement bounded lazy iterators and paged reads.
- Prove conditioned/unconditioned matching, case sensitivity, sorted reverse
  reads, filter-bound cursors, cancellation, and resource release.

## WP-006 — Full OpenFGA conformance and launcher

Status: pending.

- Run `storage/test.RunAllTests` against a real RiffDB service.
- Add an embedded OpenFGA launcher wired with `server.WithDatastore`.
- Exercise model write, tuple write, check, list objects, changes, restart, and
  crash-recovery flows through OpenFGA's API.
- Document supported configuration and explicit limits.

## WP-007 — Release readiness

Status: deferred; publishing is out of scope for now.

- Add compatibility policy, changelog, package metadata, CI, and release
  provenance.
- Do not publish until the conformance and loopback gates are green.


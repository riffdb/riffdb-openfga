# Design

## Compatibility target

This repository pins OpenFGA `v1.18.3`. The adapter is complete only when one
implementation satisfies all 21 methods in `storage.OpenFGADatastore` and the
official exported `pkg/storage/test.RunAllTests` suite against a real RiffDB
deployment.

The required domains are:

| Domain | Required behavior |
|---|---|
| Tuples | Exact and partial reads, paged reads, userset reads, reverse reads, atomic delete-before-insert writes, duplicate/missing policies |
| Changelog | A durable entry for every tuple mutation, occurrence ordering, object-type filters, horizon offsets, ascending/descending pages, ULID continuation tokens |
| Models | Write/read exact models, latest model, descending paged model history, 100-type advertised limit |
| Assertions | Atomic replacement and exact read by store and model |
| Stores | Create, soft delete, exact read, filtered/paged listing, case-sensitive identifiers |
| Lifecycle | Readiness and idempotent close behavior |

OpenFGA defaults to 100 tuple mutations per write, 100 types per model, and a
50-row page. Some direct datastore callers can request a much larger page.
The adapter must retain a finite internal result bound; the embedded launcher
will enforce compatible request limits before the datastore boundary.

## RiffDB application shape

The compiled application will own separate entities for stores, relationship
tuples, authorization models, assertion sets, and tuple changes. Store ID is
the partition key for all OpenFGA-owned data. OpenFGA identifiers remain
case-sensitive byte strings.

Relationship tuple identity is the base `(store, object, relation, user)`
triplet. A condition changes tuple payload, not identity: writing the same
triplet with a different condition is still a duplicate, and a delete without
condition removes the conditioned tuple. Condition context is deterministic
protobuf bytes with explicit size bounds.

Authorization models and assertion sets are stored as deterministic protobuf
bytes. Decoders reject malformed, non-canonical, oversized, or wrong-message
payloads. The adapter does not interpret OpenFGA authorization semantics; it
preserves the upstream protobuf exactly and lets OpenFGA own evaluation.

## Commands

All mutations are compiled operations:

| Command | Atomic responsibility |
|---|---|
| `CreateStore` | Reject collision and create the store |
| `DeleteStore` | Mark one store deleted idempotently |
| `WriteAuthorizationModel` | Persist one bounded immutable model |
| `ReplaceAssertions` | Replace the bounded assertion set for one model |
| `WriteTuples` | Validate the entire bounded request, apply deletes before inserts, enforce duplicate/missing policy, and append matching changes |

Idempotency keys are generated once per Go call and retained for all retry
attempts. RiffDB owns concurrency validation and atomic commit. Application
errors are mapped to OpenFGA's typed storage errors only after the command
outcome is certain.

## Queries

Every accepted filter shape maps to a generated named query. The initial query
catalog must cover:

- exact tuple identity;
- partial tuple reads for every upstream-supported object/relation/user shape;
- conditioned and unconditioned filters;
- userset filtering by allowed type and relation;
- reverse reads by user/userset with optional object IDs and ascending order;
- store lists by optional IDs and name;
- model history in descending ULID order;
- exact assertion-set reads; and
- tuple changes by store, optional object type, horizon, direction, and cursor.

Continuation tokens are adapter envelopes, except where OpenFGA requires a
ULID change token. An envelope binds the query identity, normalized filter,
ordering, store, and underlying RiffDB cursor. Tokens are bounded and reject
cross-query reuse.

Iterators fetch bounded pages lazily and release resources on `Stop` or after
`ErrIteratorDone`. They never materialize a whole store.

## Change ordering gate

OpenFGA requires change records in occurrence order and requires a ULID
continuation token. The tuple command therefore needs one durable ordering
fact shared by tuple mutations and their changelog rows. Before implementing
the changelog, prove that the compiled RiffDB application can derive or persist
an order that:

1. agrees with authoritative commit order across concurrent adapter processes;
2. is available to the named change query;
3. can be mapped monotonically to a valid ULID continuation token; and
4. does not depend on a client clock or process-local monotonic state for
   correctness.

If the current public RiffDB surface cannot provide that fact, stop and raise a
RiffDB capability proposal. Do not paper over the gap with client-generated
ordering.

## Consistency and ownership

The caller supplies and owns a connected `riffdb.dev/application.Session` with
the generated application identity and OpenFGA server role. `Close` releases
only adapter-owned state; session ownership will be explicit in the eventual
constructor rather than surprising the caller.

OpenFGA consistency preferences map to the strongest available named-query
semantics. Any per-call read-after-commit fence must be backed by a durable
RiffDB token, not process-local assumptions. Unsupported preferences fail
closed.

## Error mapping

The mapping layer will distinguish at least not-found, collision, invalid
write input, transactional conflict, invalid continuation token, cancellation,
deadline, unavailable, and internal corruption. It preserves `errors.Is`
behavior expected by OpenFGA while redacting internal RiffDB details.


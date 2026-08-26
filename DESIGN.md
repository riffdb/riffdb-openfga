# Design

## Compatibility target

This repository pins OpenFGA `v1.18.3`. The adapter satisfies all 21 methods in
`storage.OpenFGADatastore`. The official exported
`pkg/storage/test.RunAllTests` suite runs against the real RiffDB Go transport
through `make conformance`.

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
50-row page. The adapter preserves that 100-mutation atomic write bound while
the compiled commands enforce a 900,000-byte aggregate canonical element
budget. This covers OpenFGA's bounded request envelope without multiplying the
largest legal condition context by every list position. Some direct datastore
callers can request a much larger page.
The adapter retains a finite internal result bound and paginates larger public
requests internally.

## RiffDB application shape

The compiled application owns separate entities for stores, relationship
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
payloads. An explicitly present empty condition context is stored as the
single-byte adapter sentinel `0x00`, because an empty deterministic protobuf
`Struct` otherwise encodes to the same zero bytes as an absent optional value.
The sentinel is decoded only at the adapter boundary. The adapter does not
interpret OpenFGA authorization semantics; it preserves the upstream protobuf
exactly and lets OpenFGA own evaluation.

## Commands

All mutations are compiled operations:

| Command | Atomic responsibility |
|---|---|
| `CreateStore` | Reject collision and create the store |
| `DeleteStore` | Mark one store deleted idempotently |
| `WriteAuthorizationModel` | Persist one bounded immutable model |
| `CreateAssertionSet` / `UpdateAssertionSet` | Create or revision-guard the bounded assertion set for one model |
| `EnsureTuplePartition` / `EnsureTupleStates` | Prepare the bounded tuple partition and revision-bearing tuple states |
| `ApplyTupleMutations` | Validate the entire bounded request, apply deletes before inserts, enforce duplicate/missing policy, and append matching changes |

Idempotency keys are generated once per Go call and retained for all retry
attempts. RiffDB owns concurrency validation and atomic commit. Application
errors are mapped to OpenFGA's typed storage errors only after the command
outcome is certain.

## Queries

Every accepted filter shape maps to a generated named query. The query catalog
covers:

- exact tuple identity;
- partial tuple reads for every upstream-supported object/relation/user shape;
- conditioned and unconditioned filters;
- userset filtering by allowed type and relation;
- reverse reads by user/userset with optional object IDs and ascending order;
- store lists by optional IDs and name;
- model history in descending ULID order;
- exact assertion-set reads; and
- tuple changes by store, optional object type, durable timestamp horizon,
  direction, and cursor.

Continuation tokens are bounded adapter envelopes. Store, model, and tuple
tokens bind query identity, normalized filter, ordering, and the underlying
RiffDB cursor. Changelog tokens additionally bind the durable timestamp
horizon. Tokens reject cross-query reuse. A raw OpenFGA ULID is accepted as a
changelog start-time token for compatibility, but it is never used as the
authoritative ordering key.

Iterators fetch bounded pages lazily and release resources on `Stop` or after
`ErrIteratorDone`. They never materialize a whole store.

## Change ordering

RiffDB assigns each appended change a durable `inserted_at` value during the
atomic tuple command. Named change queries order by `(inserted_at, change_id)`
using bytewise text keys, so concurrent adapter processes share one durable
order. The first page freezes a timestamp horizon in the adapter token; later
pages use RiffDB's protected cursor and the same horizon. No client clock or
process-local monotonic state establishes correctness.

## Consistency and ownership

The caller supplies a connected `riffdb.dev/application.Session` with the
generated application identity and OpenFGA server role. `Close` releases only
adapter-owned state unless `Config.CloseSession` explicitly transfers session
ownership to the datastore.

OpenFGA consistency preferences map to the strongest available named-query
semantics. Any per-call read-after-commit fence must be backed by a durable
RiffDB token, not process-local assumptions. Unsupported preferences fail
closed.

## Error mapping

The mapping layer distinguishes not-found, collision, invalid
write input, transactional conflict, invalid continuation token, cancellation,
deadline, unavailable, and internal corruption. It preserves `errors.Is`
behavior expected by OpenFGA while redacting internal RiffDB details.

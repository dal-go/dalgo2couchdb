# dalgo2couchdb

Bounded, read-only [DALgo](https://github.com/dal-go/dalgo) adapter for **native
documents in one existing CouchDB database**. It queries application documents
as they are stored; it does not require the `__dalgo_*`/`data` envelope used by
the separate TypeScript adapter, and it never writes to CouchDB.

- Adapter version: `0.1.0` (`dalgo2couchdb`, `dal.Adapter`)
- DALgo baseline: `github.com/dal-go/dalgo v0.95.3`
- Record baseline: `github.com/dal-go/record v0.1.5`
- CouchDB API: CouchDB `3.5.x`, Mango `_find`/`_index`, native `_all_docs`
  primary enumeration and native document GET.
  Verified against the official `apache/couchdb:3.5.3` image, digest
  `sha256:d6bc2568f38d9f568afdc09cb33b871608db861201d885e34769878049da43f2`
  (CouchDB `3.5.3`, features `access-ready`, `partitioned`,
  `pluggable-storage-engines`, `reshard`, `scheduler`).
- Adapter licence: MIT (see [LICENSE](LICENSE)). CouchDB itself is Apache-2.0.

## Installation

```sh
go get github.com/dal-go/dalgo2couchdb
```

## Connect

`NewDB` validates configuration and performs no network request. It returns a
sealed `dal.DB`; optional capabilities are reached with `dal.As`.

```go
cfg := dalgo2couchdb.Config{
    // Exactly one existing database. HTTPS is required by default.
    DatabaseURL: "https://couch.example.com:6984/orders",
    // Logical DALgo collection; defaults to "orders" (the database name).
    Collection: "orders",
    // Credentials belong here, never in the URL. Called for every request.
    Headers: func(ctx context.Context) (map[string]string, error) {
        return map[string]string{
            "Authorization": "Bearer " + currentToken(ctx),
        }, nil
    },
}
db, err := dalgo2couchdb.NewDB(ctx, cfg)
if err != nil {
    return err
}
defer func() {
    if closer, ok := dal.As[io.Closer](db); ok {
        _ = closer.Close()
    }
}()
```

Use a CouchDB user with read access to that database. Metadata browsing also
needs read access to `/{db}/_index`; it does not need server administrator
rights. CouchDB 3.x databases created by an administrator can be restricted to
named members; grant the adapter's user membership in that case.

`AllowLoopbackHTTP: true` permits plain HTTP **only** for `localhost`, `127.0.0.1`
and `::1`, for local development. URL credentials, queries, fragments and
CouchDB management paths are rejected. A supplied `HTTPClient` is copied;
redirects are blocked on the copy. The adapter does not use ambient proxy
configuration.

## Native identity and values

- CouchDB `_id` is the DALgo record key. It is stable native identity; for
  example `record.NewKeyWithID("orders", "order-42")`.
- `_rev` is document metadata. `Get`/`GetMulti` return it in the record data
  map because there is no separate metadata channel in `dal.Record`. A
  structured projection exposes `_rev` only when the caller declares it.
- Native `null` and an absent field are different to CouchDB. An explicit
  `field == nil` comparison maps to native `$eq: null` and matches only
  documents that carry an explicit `null`; it does not match documents where
  the field is missing. `dal.IsNullCondition` has uniform DALgo semantics (an
  absent field is NULL) and is therefore refused before any request; see
  [structured queries](#structured-queries).
- `_id` is not duplicated inside the data map; it is the key.
- Native JSON types are preserved: 64-bit integers become `int64` where they
  fit, other numbers become `float64`, integers wider than `int64` stay
  `json.Number` rather than losing precision, and `null`, booleans, strings,
  arrays and nested objects are unchanged.
- Design/local management documents (`_id` beginning with `_`) are not exposed.
  Native primary enumeration also excludes them, and deleted documents, before
  the application offset and limit; they are never counted as results.
- Caller-supplied IDs must be non-empty UTF-8 strings under 512 bytes, must not
  begin with `_`, and must not contain control characters or `..`. Other native
  characters, including `/`, are URL-escaped on the wire.

## Reads

`Get`, `Exists` and `GetMulti` read native documents. `Get` requires a record
with a data target, as in other DALgo adapters:

```go
rec := record.NewRecordWithData(
    record.NewKeyWithID("orders", "order-42"),
    &map[string]any{},
)
if err := db.Get(ctx, rec); err != nil {
    if errors.Is(err, record.ErrRecordNotFound) { /* absent */ }
    return err
}
data := *rec.Data().(*map[string]any) // native fields plus _rev
```

`Exists` uses `HEAD` and does not download the document. `GetMulti` is bounded
by `MaxGetMany`; every key is validated before the first request. The whole
batch is one operation: all documents share one deadline, one `MaxTotalBytes`
response budget and one result order, so N documents cannot restart the limits
or retain N response bodies. A failure returns `*GetMultiError` with the index
of the first unread item; earlier records stay populated, later records are
untouched and are not requested.

Writes are not implemented. The adapter does not satisfy `dal.WriteSession`, and
`RunReadwriteTransaction` returns an error wrapping `dal.ErrNotSupported` without
calling the worker. Read-only transactions only group reads; they do not provide
snapshot isolation.

## Structured queries

Queries must target the configured logical collection and supply a **positive
bounded limit**. Supported translation is intentionally small:

- one root collection; no parent, schema, database alias, joins or subqueries
- simple top-level field names, plus `_id`/`_rev`
- comparisons `==`, `>`, `>=`, `<`, `<=` (mapped to `$eq`, `$gt`, `$gte`,
  `$lt`, `$lte`), `IN`/`NOT IN` with arrays
- `IS NOT NULL`, mapped to Mango `$ne: null` (present and not null)
- `AND`/`OR` groups with a depth/node budget
- projection of simple fields
- ordering with a single direction for all keys
- bounded `OFFSET` and `LIMIT`
- one Mango page size and page budget; pages inside one operation use CouchDB
  bookmarks
- unfiltered collection enumeration: no `WHERE`, and either no `ORDER BY` or a
  single `_id` order (ascending or descending), served by bounded native
  `_all_docs` primary-index pages

### Native primary-index enumeration

A read with **no filter** (`WHERE` absent) and either no ordering or a single
`ORDER BY _id` direction is executed deliberately against CouchDB's native
`_all_docs` primary index with `include_docs=true`. This is the only execution
path that does not need a Mango index, so a plain bounded collection read such
as `FROM orders LIMIT 5` works on an existing unindexed database.

- `_all_docs` is always available; the adapter never creates an index.
- Management documents (`_id` beginning with `_`, including `_design/...`;
  `_local/...` does not appear in `_all_docs`) and deleted documents are
  excluded **before** the logical application offset and limit. They never
  consume an offset slot or a result row, and no envelope document is invented.
- Paging uses the native `_id` as the stable **inclusive** continuation key:
  the next page starts at the last physical `_id` (`startkey`) and discards
  only that actual anchor row. If the anchor document was deleted between two
  reads, CouchDB starts at the next surviving document and the adapter keeps
  it, so a concurrent delete cannot silently drop that document. When the hard
  `MaxRowsPerPage` allows it, one lookahead row may be requested so an
  anchor-only page still advances; a request never exceeds `MaxRowsPerPage`.
  Each physical page also validates that IDs stay in native
  ascending/descending order and never repeat, so a hostile or out-of-order
  page fails instead of duplicating rows.
- A `MaxRowsPerPage` of one cannot advance past a still-present anchor without
  reopening that delete race, so such a continuation is refused explicitly
  with `dal.ErrNotSupported` instead of reporting a false EOF, duplicating a
  row or looping silently. A single page, or a continuation whose anchor was
  deleted, still completes normally at that cap.
- `ORDER BY _id` descending is requested natively with `descending=true`.
  If an endpoint does not support native descending `_all_docs`, the request
  fails explicitly; rows are never silently returned in ascending order.
- Physical pages are bounded by `MaxRowsPerPage`; all pages, including pages
  that carry only management documents, share one operation deadline, one
  `MaxPages` budget, and the operation response/row byte budgets. Exhausting a
  budget raises `ErrTooManyPages`, `ErrResponseTooLarge`, `ErrRowTooLarge` or
  `ErrTooManyRows`; it is never reported as a successful short EOF.
- The optional cross-call `MangoPager` does not expose this mode: a primary
  query passed to `ReadMangoPage` returns `dal.ErrNotSupported` before any
  request. Use `ExecuteQueryToRecordsReader` (or the recordset variant) for
  unfiltered enumeration.

Any filter, or an ordering on a field other than `_id`, keeps the Mango path
below. A filter that cannot use a Mango index is **not** rerouted to a primary
scan; it fails with `ErrIndexRequired` (or `dal.ErrNotSupported` for an
untranslatable construct).

Joins, grouping, `HAVING`, aggregate columns, subqueries, wildcard or aliased
projections, nested/dotted fields, mixed sort directions, text/raw Mango, raw
URLs and management endpoints are rejected with `dal.ErrNotSupported` **before
any request** when validated by the adapter. `IS NULL` is also rejected before
any request: DALgo defines it to match an absent field as well as an explicit
null, a JSON index only contains documents where the indexed field is present,
and a filtered query never uses the `_all_docs` primary scan. Use an explicit
`field == nil` comparison for explicit nulls, or `IS NOT NULL` for present,
non-null values. The generic DALgo aggregation fallback supplies a source limit
of `0`; this adapter refuses it rather than silently aggregating a capped
partial source.

```go
q := dal.From(dal.NewRootCollectionRef("orders", "")).NewQuery().
    WhereField("status", dal.Equal, "active").
    WhereField("total", dal.GreaterOrEqual, 100).
    OrderBy(dal.DescendingField("createdAt")).
    Limit(50).
    SelectColumns(
        dal.Column{Expression: dal.Field("number")},
        dal.Column{Expression: dal.Field("total")},
        dal.Column{Expression: dal.Field("createdAt")},
    )

reader, err := db.ExecuteQueryToRecordsReader(ctx, q)
if err != nil { return err }
defer func() { _ = reader.Close() }()
for {
    rec, err := reader.Next()
    if errors.Is(err, dal.ErrNoMoreRecords) { break }
    if err != nil { return err }
    use(rec.Key().ID, rec.Data())
}
```

`ExecuteQueryToRecordsetReader` materializes the same bounded result into a
`recordset.Recordset`. A projected query returns exactly its declared columns,
in declaration order: `_rev` (and any other undeclared native field) is
available to the adapter for identity/revision validation but is not exposed,
unless the caller declares `_rev`. An explicitly requested `_id` column is
materialized from the native record key rather than dropped, and `_id` is never
duplicated in record data. An unprojected query uses the sorted union of
returned native fields (including `_rev`) and leaves identity on the record
key. `recordset.WithName` overrides the recordset name.

### Mango indexes and sorting

Every **filtered** query always sends `allow_fallback: false`. Such a query must
be served by a real Mango index; CouchDB's implicit primary scan or a silent
fallback is not used. When no usable index exists, CouchDB's
`no_usable_index` and `invalid_index` responses both become
`dalgo2couchdb.ErrIndexRequired` with only a safe message. Sorting requires an
index with the sort fields in the same order; CouchDB Mango also requires all
sort directions to match. An `OR` whose branches need different indexes can
therefore fail with `ErrIndexRequired` even though each branch alone is
queryable.

The only native primary-index path is the explicit unfiltered enumeration
described above. It is a deliberately selected execution route for a query that
has no predicate and at most a single `_id` order; it is not a CouchDB fallback,
and no predicate is ever moved from Mango to that scan.

Create indexes outside the adapter (for example with the CouchDB HTTP API, an
administrator, or your deployment tooling). The adapter never creates indexes.

### Cross-call Mango pagination

Within one DALgo query the adapter follows bookmarks automatically. For
explicit pages across calls, use the optional `MangoPager` capability:

```go
pager, ok := dal.As[dalgo2couchdb.MangoPager](db)
if !ok { return errors.New("no pager") }

page, err := pager.ReadMangoPage(ctx, dalgo2couchdb.MangoPageRequest{Query: q})
// page.Records, page.Continuation
page, err = pager.ReadMangoPage(ctx, dalgo2couchdb.MangoPageRequest{
    Query: q, Continuation: page.Continuation,
})
```

`Query.Limit` is the page size and may not exceed `MaxRowsPerPage`. A
continuation is an opaque token bound by SHA-256 to the endpoint, logical
collection, selector, projection and order. Reusing it with a different query
returns `ErrContinuationMismatch` before any request. An offset may not be
combined with a continuation. DALgo `StartFrom`/`StartAfter` cursors are not
supported; use `MangoPager`.

`MangoPager` is Mango-only. An unfiltered query that selects native
primary-index enumeration is refused with `dal.ErrNotSupported` before any
request, so a native `_id` continuation is never misencoded as a Mango
bookmark. Read such queries with `ExecuteQueryToRecordsReader`.

## Metadata

The optional `dbschema.SchemaReader` capability is available through
`dal.As[dbschema.SchemaReader](db)`:

- `ListCollections` returns the one configured logical collection.
- `DescribeCollection` returns that name and the native `_id` primary key.
  `Fields` is empty: no schema is inferred from sampled documents.
- `ListIndexes` returns the real indexes from `/{db}/_index`. A failed or
  forbidden metadata request is an error, never an empty index list.
- `ListConstraints` and `ListReferrers` return `dbschema.NotSupportedError`
  (wrapping `dal.ErrNotSupported`); CouchDB has no SQL constraint/foreign-key
  catalog.

## Limits

All limits are finite and validated by `NewDB`. Zero selects the default;
negative or above-maximum values are rejected.

| Config | Default | Maximum | Meaning |
|---|---:|---:|---|
| `MaxRows` | 1000 | 10000 | largest accepted query `LIMIT` |
| `MaxRowsPerPage` | 100 | 1000 | rows in one Mango request or one native `_all_docs` page |
| `MaxPages` | 20 | 100 | Mango or native primary-index pages per operation, including pages that carry only management documents |
| `MaxOffset` | 10000 | 1000000 | largest query offset |
| `MaxGetMany` | 100 | 1000 | keys in one `GetMulti` |
| `MaxRequestBytes` | 1 MiB | 64 MiB | JSON request body |
| `MaxResponseBytes` | 4 MiB | 64 MiB | one HTTP response body |
| `MaxTotalBytes` | 16 MiB | 64 MiB | response bytes and retained row bytes (each bounded by this cap) per operation; `GetMulti` and all native primary-index pages are one operation |
| `MaxRowBytes` | 1 MiB | 64 MiB | one decoded document |
| `Timeout` | 30s | 2m | one HTTP request; `GetMulti` uses one deadline for the whole batch and native primary enumeration uses one deadline for all its pages |

Response byte limits are checked before JSON decoding; row and cardinality
limits are checked before a record is returned. Exceeding a budget stops the
operation; results are never silently truncated. An HTTP 200 Mango body must
contain an explicit non-null `docs` array, a native `_all_docs` body must
contain an explicit non-null `rows` array, and `_index` must contain an explicit
non-null `indexes` array; `{}`, `null` and wrong shapes are errors, while
`docs: []`, `rows: []` and `indexes: []` are legitimate empty results. A
`_all_docs` row must carry an `id`, its `key` must match when present, and
`include_docs` must return a document whose `_id` matches; a null document is
accepted only as a marked deleted row and is skipped.

Cancellation and deadlines propagate through the request context. Native primary
enumeration also owns one operation context: closing the reader or cancelling
the caller's context aborts an in-flight page request, and buffered rows do not
outlive cancellation. The adapter returns safe errors that preserve
`errors.Is(err, context.Canceled)` and
`errors.Is(err, context.DeadlineExceeded)` without exposing transport text.

## Errors

Use `errors.Is` with `dal.ErrNotSupported`, `record.ErrRecordNotFound`, and the
adapter's exported sentinels (`ErrIndexRequired`, `ErrContinuationMismatch`,
`ErrResponseTooLarge`, `ErrRowTooLarge`, `ErrTooManyRows`, `ErrTooManyPages`,
`ErrMalformedResponse`, `ErrUnsafeValue`, `ErrHeaderProvider`,
`ErrRequestFailed`, `ErrDatabaseNotFound`). `*GetMultiError` identifies the
failed batch index and unwraps to these errors. `*HTTPError` carries only a
status code. Errors never include URLs, credentials, headers, request bodies or
response bodies.

## Real local CouchDB tests

The real-service test is designed for an owned disposable container (started by
the shell recipe below). It seeds native documents, design documents and Mango
indexes (never a JSON `_id` index), creates a non-admin reader member, and
exercises real `Get`/`Exists`/`GetMulti`, filter/sort/projection (including
id-only, explicit `_rev`, mixed order and wildcard), several bookmark pages,
native null versus `IS NOT NULL` and the `IS NULL` refusal, the schema/index
endpoint, an index-required failure and bad credentials, and bounded native
primary enumeration across several physical pages that contain management
documents and a deleted document. Each run creates its own uniquely suffixed
database and drops it again in test cleanup.

```sh
RUN=$(date +%s)
NAME=datatug-couchdb-$RUN
IMAGE='apache/couchdb:3.5.3@sha256:d6bc2568f38d9f568afdc09cb33b871608db861201d885e34769878049da43f2'

docker run -d --name "$NAME" -p 127.0.0.1::5984 \
  -e COUCHDB_USER=datatug-admin \
  -e COUCHDB_PASSWORD=test-admin-password \
  "$IMAGE"

PORT=$(docker port "$NAME" 5984/tcp | sed 's/.*://')
for i in $(seq 1 60); do
  curl -fsS "http://127.0.0.1:$PORT/_up" >/dev/null && break
  sleep 1
done

COUCHDB_TEST_URL="http://127.0.0.1:$PORT/datatug_couchdb_test" \
COUCHDB_TEST_ADMIN_USER=datatug-admin \
COUCHDB_TEST_ADMIN_PASSWORD=test-admin-password \
COUCHDB_TEST_READER_USER=datatug-reader \
COUCHDB_TEST_READER_PASSWORD=test-reader-password \
go test -race -count=1 ./...

docker rm -f "$NAME"
```

The test refuses to touch a database whose name does not start with
`datatug_couchdb_test`. The configured name is a disposable prefix: each run
creates `<name>_run_<id>` and drops it in cleanup. Do not point it at
production. Unit tests use `httptest` and need no Docker.

## CI

`.github/workflows/ci.yml` runs unit/race tests, focused real CouchDB tests
(pinned `apache/couchdb:3.5.3` service), `go vet` and a formatting check. GitHub
Actions are pinned by commit SHA.

## Unsupported by design

- writes, upserts, deletes, bulk mutations, RW transactions, MVCC conflict
  handling, and any CouchDB management endpoint
- raw URLs, raw Mango bodies, raw `_all_docs` requests from callers, `_all_dbs`,
  count queries and unbounded scans; the adapter's own bounded `_all_docs`
  primary enumeration is not a public raw endpoint
- joins, aggregation, grouping/`HAVING`, subqueries, text search and full-text
  indexes
- nested/dotted field paths, wildcard projections, projection aliases, and
  mixed sort directions
- cross-database queries, partitioned-database routing, views/`_view`,
  replication and changes feeds
- automatic index creation

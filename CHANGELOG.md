# Changelog

All notable changes to this project are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [1.2.0] - 2026-07-28

### Added

- Parallel (multi-connection) bulk insert via `ParallelBulkInserter<T>` and the
  `ClickHouseDataSource` entry points `CreateParallelBulkInserterAsync<T>` and the
  one-shot `BulkInsertAsync<T>` (`IEnumerable<T>` / `IAsyncEnumerable<T>`). Rows
  pushed via `AddAsync` are fanned out through a bounded channel to
  `DegreeOfParallelism` workers, each running an independent single-connection
  insert over its own pooled connection ("pipe"), with backpressure, error
  aggregation, and `RowsWritten` reporting. Configured by `ParallelBulkInsertOptions`
  (`DegreeOfParallelism`, `BatchSize`, `ChannelCapacity`, `Roles`, `UseSchemaCache`,
  `QueryId`). The insert is intentionally not atomic and not idempotent on retry,
  and exposes **no** `DeduplicationToken` — work-stealing fan-out cannot make a
  dedup token sound (handle idempotency above the inserter via a staging-table swap
  or an idempotent engine). `DegreeOfParallelism` above the data source's
  `MaxPoolSize` is rejected up front. See `docs/bulk-insert.md`.
- Multidimensional array support for `Array(Array(T))` (and deeper) columns.
  Properties typed as rectangular `T[,]` / `T[,,]` (any rank ≥ 2) are now
  materialized via a jagged→rectangular conversion at the read boundary,
  validating uniform shape and reporting the offending row index in a
  `ClickHouseTypeConversionException` on mismatch. Surfaces through the
  reflection-based typed row mapper (`QueryStreamAsync<T>`, LINQ `Table<T>`
  result projection) and the ADO.NET reader's `GetFieldValue<T>()` when `T`
  is a rectangular array type. Jagged `T[][]` / `T[][][]` continues to work
  unchanged for ragged data.
- `CH.Native.Dapper.ClickHouseDbConnectionDapperExtensions` — Dapper-shaped
  fast-path methods on `ClickHouseDbConnection`: `QueryAsync<T>`,
  `QueryStreamAsync<T>`, `QueryFirstAsync<T>`, `QueryFirstOrDefaultAsync<T>`,
  `QuerySingleAsync<T>`, `QuerySingleOrDefaultAsync<T>`. Bypasses Dapper's
  compiled row mapper and routes through CH.Native's typed-accessor
  `TypeMapper<T>` instead. Variables typed as `ClickHouseDbConnection`
  automatically pick this path via C# extension-method resolution.
- `CH.Native.Dapper.IDbConnectionDapperExtensions` — namespace-swap drop-in
  for the standard Dapper surface. Replace `using Dapper;` with
  `using CH.Native.Dapper;` and the fast path resolves automatically for
  ClickHouse connections when typed as `IDbConnection` (e.g. in DI). Row-
  mapping methods dispatch to the fast path for `ClickHouseDbConnection`
  receivers and delegate to `Dapper.SqlMapper` for everything else.
  Non-mapping methods (`ExecuteAsync`, `ExecuteScalarAsync`,
  `QueryMultipleAsync`, dynamic `QueryAsync`, all sync variants) are thin
  delegates to Dapper. Import only one of `using Dapper;` /
  `using CH.Native.Dapper;` to avoid compile-time ambiguity on
  `IDbConnection`-typed calls.
- `ITypedColumn.IsNull(int index)` — new default-interface method for
  null-checking without materialising the value. `TypedColumn<T>` overrides
  with a JIT-folded short-circuit (`return false` for non-nullable value
  types), eliminating the boxing previously paid by
  `IsDBNull → GetValue is null`.
- **`Nothing` type support** — bare `SELECT NULL` (`Nullable(Nothing)`) and
  `SELECT []` (`Array(Nothing)`) now read as `null` / empty array, matching
  the CLI and the official driver. Previously these threw
  `NotSupportedException` *and* poisoned the connection — notable because
  ORMs and health checks emit bare `SELECT NULL` probes.
- **Interval type support** — all eleven `Interval*` types
  (`IntervalNanosecond` … `IntervalYear`, the server's full set per
  `system.data_type_families`) decode to the new public
  `CH.Native.Data.ClickHouseInterval` struct (count + `IntervalUnit`).
  Deliberately not mapped to `TimeSpan`: Month/Quarter/Year are calendar
  units with no fixed duration — `ToTimeSpan()` converts the time-based
  units exactly (nanoseconds truncate to 100-ns ticks) and throws for
  calendar units; check `IsCalendarUnit` first. Previously threw
  `NotSupportedException` and poisoned the connection.
- **Raw byte access for `String` columns** — ClickHouse `String` is an
  arbitrary byte sequence; bytes that aren't valid UTF-8 are replaced with
  U+FFFD in the `string` view (unchanged default). Under
  `StringMaterialization=Lazy` the exact stored bytes are now recoverable:
  `GetFieldValue<byte[]>(ordinal)` on the streaming row and ADO reader,
  backed by new public `RawStringColumn.GetRawBytes(int)` /
  `GetBytesCopy(int)` and `NullableRawStringColumn.GetBytesCopy(int)`
  (null rows surface as SQL null).
- **Full-precision access for `DateTime64(8/9)`** — the `DateTime` view
  truncates to 100-ns ticks (a CLR ceiling; scales 0–7 are exact). Block
  reads of scales 8–9 now retain the raw Int64 wire value (new public
  `CH.Native.Data.DateTime64RawColumn`), reachable via
  `GetFieldValue<long>(ordinal)`, `ExecuteScalarAsync<long>(...)`, and
  `long` properties in typed row mapping — for scale 9 the value equals
  `toUnixTimestamp64Nano`. Not wired for `Nullable(DateTime64(8/9))`;
  use `toUnixTimestamp64Nano(col)` server-side there.

### Breaking

- **Renamed** `ClickHouseConnection.QueryAsync<T>` and the matching
  parameterised extensions on `ClickHouseConnectionExtensions` to
  `QueryStreamAsync<T>` (and the untyped `QueryAsync` → `QueryStreamAsync`).
  This frees the `QueryAsync<T>` name for the new Dapper-shaped extension on
  `ClickHouseDbConnection`. Direct native-API users migrate via a one-token
  rename; the ADO.NET surface (`ClickHouseDbCommand.ExecuteReaderAsync` etc.)
  is unaffected.

### Changed

- **Connection reusability is now derived from protocol evidence** instead of
  per-path bookkeeping. Every wire conversation tracks two facts — "did we
  write non-cancel bytes?" and "have we consumed a full response terminator
  since?" — and a conversation that ends without proof of a terminator marks
  the connection broken rather than letting it look reusable. Consequences:
  - Dead sockets (server FIN/RST, truncation, network drops mid-query) are
    reliably discarded by the pool instead of being re-issued to the next
    caller; abandoned response bytes can never be read by a later query as its
    own result (previously possible after cancellation/timeouts — silent wrong
    answers).
  - Cancelling a query now drains the wire to the server's response terminator
    BEFORE the `OperationCanceledException` reaches the caller, on every entry
    point (previously only the scalar/non-query paths, behind a race-prone
    guard). For queries the server aborts promptly this adds milliseconds; for
    non-interruptible server work the cancellation surfaces when the server
    responds. `QueryTypedAsync<T>` also realigns on early `break` and on
    consumer exceptions.
  - A `CommandTimeout` on the reader path now leaves the connection genuinely
    reusable (previously the next query could read the timed-out query's
    response).
  - Server-side errors are unaffected: a consumed exception envelope still
    leaves the connection reusable.
  - `ChangeRolesAsync` now claims the connection's busy slot like every other
    entry point; calling it concurrently with a running query throws
    `ClickHouseConnectionBusyException` instead of interleaving writes.
- Wire-declared block header counts are validated against the bytes actually
  available before any count-sized allocation, closing a path where a corrupt
  or hostile stream could trigger multi-GB allocations from a 5-byte varint.
- Pool hardening: a connection returned concurrently with `DataSource`
  disposal can no longer leak as an undisposed socket; a cancelled rent no
  longer evicts a healthy idle connection; a failed physical open no longer
  leaks the connection object; pool disposal no longer logs a spurious
  `PrewarmFailed` when it races a warming pool.
- `BulkInserter<T>` and `DynamicBulkInserter` now initialize lazily on first use:
  the INSERT query, schema resolution, and connection busy-slot claim happen at
  the first `AddAsync`/`AddRangeAsync`/`AddRangeStreamingAsync` call instead of
  requiring an explicit `InitAsync()`. `InitAsync` remains available as an
  optional eager-validation step and is now **idempotent** — calling it twice
  (or after lazy init) is a no-op instead of throwing `InvalidOperationException`
  ("already initialized"). Behavioral consequences:
  - Pre-init `AddAsync`/`FlushAsync`/`CompleteAsync` no longer throw
    "must be initialized"; initialization errors (missing table, permissions,
    busy connection, schema mismatch) surface from the first add call when
    `InitAsync` is not used. Exception types are unchanged.
  - A failed initialization (explicit or lazy) leaves the inserter retryable
    on the same connection whenever the failure leaves the wire at a clean
    protocol boundary — a pre-wire error (busy connection, invalid
    `ColumnTypes`, a token already cancelled at entry) or a server rejection of
    the INSERT statement (missing table, permission denied, unknown column,
    which come back as a clean server-exception envelope). Cancellation *after
    the INSERT is in flight* is the exception: it cannot cleanly drain the open
    INSERT, so the connection is marked broken and recovery needs a fresh
    `ClickHouseConnection` — but even then the inserter is no longer falsely
    latched as "complete-cancelled".
  - `CompleteAsync()`/dispose on a never-initialized inserter with zero rows is
    a silent no-op that never contacts the server (previously threw). The
    `ClickHouseConnection.BulkInsertAsync` convenience methods still validate
    the target table even for empty sources.
  See the "Lazy initialization" section of `docs/bulk-insert.md`.
- `TypeMapper<T>` rewritten to compile per-property
  `Expression<Action<T, ClickHouseDataReader>>` delegates that call
  `reader.GetFieldValue<TProp>(ordinal)` directly. For well-known primitive
  property types, this skips the legacy `GetValue → box → cast` round trip
  entirely. Properties whose CLR type doesn't match column storage (enums,
  multi-dim arrays, `ClickHouseMap`/`Dictionary` reshaping) still flow
  through the existing `GetValue + ConvertValue` slow path.
- `ClickHouseDataReader.GetFieldValue<T>(int)` adds a typed fast path —
  when column storage is `TypedColumn<T>` for the exact requested `T`, reads
  via the typed indexer with no boxing.
- `ClickHouseDataReader.IsDBNull(int)` now routes through
  `ITypedColumn.IsNull` instead of `GetValue is null`.
- `ClickHouseDbDataReader.Read()` (sync) adds a fast path that bypasses
  `Task.Run` when no `SynchronizationContext` / non-default `TaskScheduler`
  is captured — covers ASP.NET Core, console apps, hosted services, and
  unbuffered Dapper. UI-thread / classic-ASP.NET callers still pay the hop.
- `ClickHouseDbDataReader.ReadAsync()` adds a cached `Task<bool>` fast path
  for synchronously-completing reads — avoids the async-state-machine
  allocation per row when the inner reader returns from a buffered block.
- `DateTime64(8/9)` columns materialise as `DateTime64RawColumn` instead of
  `TypedColumn<DateTime>` (to retain the raw Int64 — see Added). Row values
  and `GetValue` still return the truncated `DateTime`; only code that
  introspects `ITypedColumn` storage types observes the difference. Scales
  0–7 are unchanged.
- Benchmark methodology: the bulk-insert comparison
  (`BulkInsertComparisonBenchmarks`) now configures CH.Native and
  `ClickHouse.Driver` with a matched batch size equal to the row count
  (a single batch per run), so the two clients are compared on identical
  batching instead of their differing defaults (CH.Native 10K, Driver 100K).
  Updated matched-batch bulk-insert numbers (1M rows): CH.Native
  **132 ms / 7.7 MB**, `ClickHouse.Driver` 193 ms / 92 MB,
  `Octonica.ClickHouseClient` 1,530 ms / 28 MB. Allocations for CH.Native are
  higher than its pooled-small-batch default by design — a single 1M-row batch
  buffers every column at once. See `docs/performance-comparison.md`.

### Removed

- The `CH_WIRE_DUMP` debug hook (env-gated hex dumps of insert traffic to a
  hardcoded `/tmp` path).

### Fixed

- **`QueryStreamAsync<T>` / `QueryTypedAsync<T>` now return values for scalar
  `T`.** Both APIs silently yielded `default(T)` for every row when `T` was a
  scalar rather than a POCO — `int`, `long`, `ulong`, `double`, `decimal`,
  `bool`, `Guid`, `DateTime` and `Nullable` variants all came back as zero,
  and `string` threw `InvalidOperationException` ("constructor parameter
  'value' does not match any column") instead. Both row mappers bind *writable
  properties* to columns and a scalar has none, so no binding was produced and
  the decoded column value was never read; `string`, having no parameterless
  constructor, fell through to the args-constructor path and tried to bind
  `String`'s own `value` parameter. A scalar `T` now binds to the row's first
  column, matching `ExecuteScalarAsync`, with SQL NULL yielding `default(T)`
  as the property-setter path already did. Fixed in both mappers —
  `TypeMapper<T>` (behind `QueryStreamAsync`) and `ReflectionTypedRowMapper<T>`
  (behind `QueryTypedAsync`, via `TypedRowMapperFactory`). Enum and
  `Nullable<enum>` targets are included. POCO, record and anonymous-type
  mapping is unchanged.
- **`LowCardinality(Nullable(value-type))` no longer drops NULLs.** A NULL in a
  `LowCardinality(Nullable(Int64 / Date / Enum8 / …))` column read back as
  `default(T)` (`0`, epoch) with `IsDBNull` returning false — so a `long?`
  property got `0` instead of `null` and a real `0` was indistinguishable from a
  lost NULL. The top-level/Tuple read path now returns a null-aware
  `DictionaryEncodedColumn` (`IsNull`/`GetValue` honor the index-0 null
  sentinel), and — because composite readers (Array/Map/Nested) build element
  storage from the element reader's CLR type — a new internal
  `LowCardinalityNullableColumnReader<T>` (genuinely `IColumnReader<T?>`) is used
  for the value-type case so nested `Array(LowCardinality(Nullable(Int64)))`,
  `Map(String, LowCardinality(Nullable(Date)))`, etc. carry NULLs and
  `GetFieldType` reports the nullable CLR type. `LowCardinality(Nullable(String))`
  and non-nullable `LowCardinality` are unchanged.
- **Compressed multi-chunk blocks no longer rejected as malformed.** The
  block-header count guard threw `ClickHouseProtocolException` inside the
  compressed accumulate loop, whose "need more decompressed data" retry
  contract is `catch (InvalidOperationException)` — so a legitimate block
  whose minimum footprint (rows × columns) exceeded the first ~1MB
  decompressed chunk (e.g. default 65,409-row blocks with 17+ columns)
  failed the query and condemned the connection. The guard now throws an
  internal `ClickHouseCountGuardException` (a plain internal exception, **not**
  part of the public `ClickHouseProtocolException` hierarchy, which stays
  `sealed`); when a caller passes `retryable: true` the accumulate loops catch
  it and read the next chunk, so the count-sized allocation stays deferred
  until real bytes back the declared count while pre-scanned (uncompressed)
  paths keep failing fast with the terminal `ClickHouseProtocolException`. The
  guard also carries its byte deficit so the loop skips re-parse attempts until
  enough new chunks have accumulated, instead of throwing once per chunk.
- **Compressed blocks fragmented mid-chunk are no longer mis-condemned.** The
  compressed pre-scan validates only that the *first* chunk is fully buffered,
  so the pipe can hand the parser a buffer ending 0–16 bytes into a later
  chunk under ordinary TCP fragmentation. The accumulate loop then exited into
  a terminal `InvalidDataException` (recognising a chunk header needs ≥17
  bytes) and condemned a healthy connection. A sub-chunk tail is now treated as
  need-more-data (retried once more bytes arrive); only a proven non-chunk tail
  is terminal. Both compressed read paths were deduplicated into a single
  `ReadCompressedTypedBlock` helper so the retry contract lives in one place.
- **An unsatisfiable block header no longer hangs the read pump.** Treating a
  sub-chunk tail as need-more-data (above) removed the last terminal exit for a
  block that *cannot* be parsed: after the final Data block the tail is the
  one-byte EndOfStream, always under the 17 bytes needed to recognise a chunk
  header, so a corrupt or hostile header was reported as "more chunks are
  coming". The server had already sent everything, so the pump waited on bytes
  that never arrived — re-decompressing every accumulated chunk per retry — until
  the caller's timeout fired, where previously it failed fast. The accumulate
  loop cannot distinguish "the counts are bogus" from "the rest is still in
  flight" by inspecting the tail (a chunk header opens with checksum bytes, so
  byte-sniffing for a message boundary would misread ~1 in 256 legitimate
  truncations), so `ProtocolGuards` now applies two verdicts that no amount of
  further data can overturn: a declared column count above 65,536 is terminal
  regardless of `retryable`, and a shortfall stays retryable only while the
  required byte count is one the int-indexed accumulator could ever hold
  (≤ 2 GiB). Legitimate multi-chunk blocks are unaffected.
- **Cancellation no longer drains against a server that owes nothing.** The
  post-`OperationCanceledException` drain gates checked only "did this
  conversation write" — a cancel landing after a nested role-sync round trip
  proved the boundary (or after a write faulted mid-flight, leaving the
  server without a complete query) drained a silent wire for the full 30 s
  cap and then wrongly poisoned a healthy connection. All abnormal-exit
  decisions now consume one shared predicate, `WireIndeterminate`
  (`_isOpen && _conversationWrote && !_boundaryProven && !_protocolFatal`),
  and a faulted/cancelled non-cancel write marks the connection
  protocol-fatal immediately so those exits fail fast.
- **Cancelled bulk-insert flush no longer stalls or masks the cancellation
  at dispose.** The inserters' post-`OperationCanceledException` cleanup gated
  on `WireIndeterminate`, which is false once a faulted write has marked the
  connection protocol-fatal — so `Abort()` never ran, `DisposeAsync` then sent
  an empty block and performed an *uncancellable* end-of-stream read on the
  truncated wire (stalling until the server timed out) and threw "rows are
  LOST" over the caller's cancellation. Cleanup now gates on the wider
  `WriteNeedsCancelCleanup` (wrote bytes, no proven boundary — regardless of
  the fatal latch), so `Abort()` always runs and dispose never touches a
  condemned wire; the drain remains a no-op on a fatal connection. Applies to
  both `BulkInserter<T>` and `DynamicBulkInserter`.
- **Cancelling a bulk insert during `CompleteAsync` no longer discards a
  healthy connection.** When the token fired inside `CompleteAsync`'s internal
  flush, the cancel-drain often proved a clean boundary (the server acked the
  Cancel with `EndOfStream`), yet the resulting `OperationCanceledException`
  fell through to the generic catch that calls `MarkProtocolFatal`, condemning
  a connection the drain had just certified reusable. `CompleteAsync` now
  rethrows a non-indeterminate cancellation without poisoning, so the
  connection returns to the pool. Applies to both inserters.
- **Cancelled validate-on-rent no longer strands, evicts, or churns
  connections.** The pool's cancelled-rent push-back now runs the connection
  through the same `TryRepoolAsync` pipeline as `ReturnAsync`: session state is
  reset *before* the `CanBePooled` check — so a role-configured pool no longer
  discards every connection on which the validate ping issued `SET ROLE`
  (`_rolesExplicitlySet` latches `CanBePooled` false until reset) — a fresh
  `LastUsedAt` is stamped so the connection isn't immediately culled as
  idle-expired on the next rent, and `_disposed` is re-checked after the push
  so an entry that raced pool disposal is drained rather than stranded as a
  live socket. The push-and-drain race close now lives in one shared helper.
  The salvage reset on this cancelled path is bounded (linked to pool disposal,
  5 s cap) so a wedged server can't strand the cancelling caller on an
  uncancellable `SET ROLE DEFAULT` read.
- **Reused caller-supplied query ids can no longer poison a successor
  query.** `ExitBusyResolve` owner-gating was query-id string equality, so
  disposing a completed reader while a later query reused the same explicit
  `queryId` released the successor's busy slot mid-flight and convicted its
  in-flight evidence. Conversations now carry a monotonic epoch captured at
  `EnterBusy`; readers and bulk inserters resolve with their epoch and a
  mismatch no-ops.
- **Prewarm failures from user-supplied open code are logged again.** The
  prewarm loop's `catch (ObjectDisposedException)` (added for the
  clean-shutdown race) swallowed genuine failures — e.g. a disposed client
  certificate or a faulty `ConnectionFactory` — leaving the pool silently
  unseeded. The catch is now gated `when (_disposed)`; everything else
  falls through to the `PrewarmFailed` logging catch.
- `DateTime` columns with an explicit timezone (e.g. `DateTime('UTC')`) no
  longer silently materialise as `default(DateTime)` when mapped to a
  non-nullable `DateTime` property via `QueryStreamAsync<T>`. The typed
  accessor path now correctly converts `DateTimeOffset → DateTime.UtcDateTime`,
  matching the documented behaviour of `GetFieldValue<DateTime>`.
- **Bulk insert into `BFloat16` columns wrote a malformed block.** The direct
  column extractor dispatched on CLR type only, so a `float` property wrote
  4-byte `Float32` payloads into the 2-byte column — desyncing the block
  stream and producing a misleading server error 261 (`Unknown BlockInfo
  field number`). The `float` extractor now branches on the column type and
  writes the truncated 2-byte bfloat16 form (matching the server-side cast),
  including `Nullable(BFloat16)`.
- **Unsupported-column-type failures now fail clean instead of leaving a
  poisoned connection.** Column data on the wire is not length-prefixed, so
  a response containing an undecodable column type (in practice: exotic
  `AggregateFunction` state formats) still costs the connection — but now:
  the original `NotSupportedException` states that the connection has been
  closed (inner exception preserved); the close is eager (`State == Closed`,
  `StateChange` fires, server session released); the next use reports
  `"Connection is broken: a previous response could not be fully read…"`
  instead of a generic not-open error; and a pooled `ClickHouseDataSource`
  discards the connection rather than re-renting it. Previously the
  connection lingered open-but-broken until its next use, which failed with
  a message that didn't name the cause.
- **Pooled bulk inserters no longer leak their rented connection.**
  `ClickHouseDataSource.CreateBulkInserterAsync` (both the typed
  `BulkInserter<T>` and the `DynamicBulkInserter` overloads) rented a pooled
  connection but nothing ever returned it: neither inserter's `DisposeAsync`
  disposed the connection, so the pool slot stayed `Busy` for the lifetime of
  the data source and a loop of create/dispose exhausted the pool. Pooled
  inserters now take explicit ownership of the connection and dispose it on
  `DisposeAsync`, firing the pool-return hook. Inserters built through the
  public constructors are unchanged — the caller still owns the connection
  and it is left open.

### Performance

- Materialized 1M-row read via `QueryStreamAsync<T>`: **265 MB → 171 MB
  (-35%)**, **247 ms → 192 ms (-22%)**, measured against
  `LargeResultSetBenchmarks`.
- Streaming 1M-row read in lazy-string mode reading a single value-type
  column: **23 MB → 128 KB (-99%)** — typed-accessor fast path removes the
  remaining per-row boxing on the column the consumer touches.
- Dapper users (via new `CH.Native.Dapper` extensions) on 1M-row reads:
  **117 MB / 143 ms** (buffered `QueryAsync<T>`), **101 MB / 104 ms**
  (`QueryStreamAsync<T>`). Beats `ClickHouse.Driver`'s comparable Dapper
  path (172 MB / 209 ms) by 30-42% memory and 1.6-2.0× time.
- Bulk-insert path untouched — pre-existing dominance preserved
  (`BulkInserter<T>` 1M rows: 99 ms / 405 KB, vs `ClickHouse.Driver`
  929 ms / 97 MB).

### Internal / CI

- Cross-client smoke suites
  `CrossClient*IT` pattern: a full-type-matrix roundtrip through the
  `clickhouse-client` CLI (exec'd inside the test container) in both insert
  directions, an official-`ClickHouse.Driver`-writes / CH.Native-reads
  suite (incl. timezone/DST scenarios), and three-way behavior-comparison
  tests pinning where the clients differ. All comparisons assert against
  anchored expected values, so a failure identifies which client deviates.
- Insert×read parity suite: each type written three ways (CLI / CH.Native
  bulk / driver bulk) into one table, read back through BOTH .NET clients
  against the same anchors — proving values are insert-source-independent.
  Comparison coverage extended with parameter-binding fidelity (the text
  path flushes `double.Epsilon` to zero in both clients; binary bulk insert
  is exact), `DateTime.Kind` write semantics (Unspecified is taken as UTC
  in both clients; Local converts), silent decimal scale truncation on
  write (both clients), and representation pins for Int128/UInt128, Time,
  geo, decimals, and negative Enum16 members.
- New "Gotchas" section and per-type detail (escape hatches, ranges,
  `Decimal256` → `ClickHouseDecimal` correction) in `docs/data-types.md`,
  with a contents index.

## [1.1.1] - 2026-05-09

### Added

- `InsertAsync<T>` extension methods on `IQueryable<T>` for the LINQ table
  handle returned by `connection.Table<T>()`. Three overloads cover single
  records (`InsertAsync(T row, ...)`), in-memory collections
  (`InsertAsync(IEnumerable<T> rows, ...)`), and async streams
  (`InsertAsync(IAsyncEnumerable<T> rows, ...)`). All three delegate to the
  existing `BulkInsertAsync<T>` plumbing — schema cache, roles, query id,
  batch size, and telemetry are inherited unchanged. Single-record inserts
  still open a fresh INSERT context per call (handshake + commit), so callers
  on hot paths should prefer the collection overload or `BulkInserter<T>`.
- `ClickHouseDataSource.Table<T>()` and `Table<T>(string tableName)` instance
  methods, mirroring the existing connection-side LINQ entry point. The
  returned queryable rents a pooled connection for the lifetime of each
  enumeration (reads) and each `InsertAsync` call (writes), then returns
  it to the pool — the handle itself does not pin a connection, so it
  composes naturally with concurrent service code.

- `DynamicBulkInserter` — POCO-less bulk-insert API. Rows are supplied as `object?[]`
  arrays whose element order matches a caller-supplied `columnNames` list. Mirrors the
  `Init` / `Add` / `Flush` / `Complete` lifecycle of `BulkInserter<T>`. Convenience
  overloads on `ClickHouseConnection`, `ClickHouseDataSource`, and `ResilientConnection`
  expose `CreateBulkInserter` (factory) and `BulkInsertAsync` (one-shot).
- `BulkInsertOptions.ColumnTypes` — pre-supplied column types keyed by column name
  (case-insensitive). When set and covering every column in `columnNames`, the dynamic
  inserter skips the server schema-probe round-trip. Honored by the dynamic
  (non-generic) bulk-insert API only; the POCO path always probes.
- Qualified `database.table` names in `BulkInserter<T>` and `DynamicBulkInserter`.
  Qualified strings are split on the single dot and rendered as `` `db`.`table` `` in
  the emitted `INSERT` SQL, addressing the named database directly. Sibling
  `(database, tableName)` overloads on `BulkInserter<T>`, `DynamicBulkInserter`,
  `ClickHouseConnection.CreateBulkInserter` / `BulkInsertAsync`,
  `ClickHouseDataSource.CreateBulkInserterAsync`, and
  `ResilientConnection.BulkInsertAsync` provide an explicit escape hatch for tables
  whose name itself contains a dot.
- `ClickHouseIdentifier.QuoteQualifiedName(string)` helper.
- `ClickHouseConnection.InvalidateSchemaCache(string database, string tableName)`
  overload; the existing single-argument form learned to parse `database.table`.

- `samples/CH.Native.Samples.Queries/` — single multi-flavour console project
  demonstrating every supported query path: scalar, data-reader, raw rows,
  typed (reflection + high-perf), parameterised, LINQ basics / aggregates /
  `Final` / `Sample`, ADO.NET, Dapper, pooled `ClickHouseDataSource`, resilient
  multi-host `ResilientConnection`, `IProgress<QueryProgress>` + cancellation,
  and a realistic log-analytics dashboard. Each flavour threads through
  cross-cutting plumbing (parameters, `CancellationToken`, custom `queryId`,
  progress) so the surface is visible end-to-end. Mirrors the layout of
  `CH.Native.Samples.Insert` (commit #27).
- `samples/CH.Native.Samples.QuickStart/` — minimal runnable counterpart to
  `docs/quickstart.md`. A single linear console script (open connection,
  scalar query, `CREATE TABLE`, bulk insert, typed `await foreach`, drop) so
  first-touch users have an executable on-ramp alongside the doc.
- `samples/CH.Native.Samples.Hosting/` — ASP.NET sample that merges the
  former `CH.Native.Samples.Authentication` and
  `CH.Native.Samples.DependencyInjection` projects. Combines `AddClickHouse`
  + keyed services + credential providers + health checks + bulk insert with
  endpoint probes for all four auth methods (password / JWT / SSH / mTLS),
  all running against the docker overlay (`./docker/setup.sh` + `docker compose up`)
  inherited from the auth sample. The keyed `mtls` / `ssh` `Demo*Provider`
  classes now read from the docker-generated client cert and SSH key, so those
  DataSources actually handshake against the local server. Adds a per-request
  role-activation pattern via `ClickHouseConnection.ChangeRolesAsync` (the
  pool discards the modified connection on return — the documented trade-off
  for per-request RBAC against a pooled DataSource).

### Changed

- Per-connection bulk-insert schema cache is now keyed by
  `(Database, Table, ColumnListFingerprint)`. Two genuinely different tables in
  different databases (e.g. `db1.events` and `db2.events`) hash to distinct keys
  and never collide. An unqualified table name resolves to the connection's default
  database, so callers that mix qualified and unqualified forms for the same logical
  table still hit the same cache entry.
- Bulk-insert telemetry now emits a `db.clickhouse.database` tag/metric label
  alongside the existing `db.clickhouse.table`. The table tag carries the
  unqualified table name regardless of whether the caller passed a qualified or
  unqualified form, so dashboards grouped on it stay coherent.

- Replaced the three small query-focused sample projects
  (`CH.Native.Samples.GettingStarted`, `CH.Native.Samples.LinqQueries`,
  `CH.Native.Samples.DapperIntegration`) with the unified
  `CH.Native.Samples.Queries` above. The new project subsumes their content
  and adds ~12 additional flavours.
- Replaced `CH.Native.Samples.Authentication` and
  `CH.Native.Samples.DependencyInjection` with the unified
  `CH.Native.Samples.Hosting` above. The docker overlay (`docker/setup.sh`,
  `docker-compose.yml`, role/cert/SSH provisioning) moved into the new
  project so a single `docker compose up` powers every endpoint.

### Behaviour change

- Anyone currently passing `"database.table"` as the `tableName` argument to
  `BulkInserter<T>` was either (a) getting a server error because `database.table`
  doesn't exist as a literal table name in the connection's default database, or
  (b) hitting a literal table whose name happens to be `database.table`. Case (a)
  is silently fixed — the rendered SQL now correctly addresses `database`'s
  `table`. Case (b) is a behaviour change: that user must now use the explicit
  `(database, tableName)` overload to address a table whose name literally contains
  a dot.

## [1.1.0] - 2026-05-06

### Added

- Support for additional primitive ClickHouse types (#11).
- Optional schema caching to avoid re-parsing column metadata on hot paths (#12).
- Additional authentication options (#13).
- Geospatial type support: `Point`, `Ring`, `Polygon`, `LineString`, `MultiLineString`, `MultiPolygon` (#15).
- Enhancements to `Variant`, `Dynamic`, and `JSON` type handling (#16).
- Connection pooling (#18).
- Side-project `CH.Native.Dapper` for Dapper-friendly extensions (#19).
- Better `CancellationToken` propagation across the public API (#23).
- `PublicAPI.Shipped/Unshipped.txt` baselines via `Microsoft.CodeAnalysis.PublicApiAnalyzers` so future surface drift is caught at build time (#24).

### Changed

- Improved logging and corrected query tracking (#14).
- Hardened type system: `NullableInnerValidator`, `LowCardinalityInnerValidator`, and `VariantArmValidator` centralise composite-type rejection rules so invalid schemas fail fast at the factory with a clear `FormatException` instead of being rejected by the server (or, worse, accepted with inconsistent wire bytes) (#19).
- Connection thread-safety fixes (#19).
- Fixed `DateTime64` nanosecond truncation (#19).
- Guards added for integer overflow paths and decoder byte caps (#19).
- Ensured pooled reader buffers are returned on failure paths (#19).
- LINQ escaping fix and broader LINQ test coverage (#19).
- Updated documentation across quickstart, configuration, authentication, bulk insert, connection pooling, ADO/Dapper, DI, LINQ, resilience, telemetry, and data types (#25).

### Breaking

- **Internal-implementation types tightened from `public` to `internal`** (#24). Affected types are protocol/serialization internals — column readers, writers, and skippers (`Int32ColumnReader`, `StringColumnWriter`, `FixedSizeColumnSkipper`, etc.), protocol messages (`QueryMessage`, `ClientHello`, `DataMessage`, `ExceptionMessage`, ...), `CityHash128`, the LZ4/Zstd compressors, and related helpers. The supported high-level surface (`ClickHouseConnection`, `BulkInserter<T>`, ADO.NET wrappers, LINQ provider, resilience, telemetry) is unchanged. If you were depending on any of the now-internal types directly, you will need to refactor onto the supported API.

### Internal / CI

- Per-type smoke tests (#17).
- System tests and additional fixes (#20).
- Coverlet code coverage and scheduled system-test runs (#21).
- CI workflow tidy-up (#22).

## [1.0.0] - 2026-03-01

Initial public release.

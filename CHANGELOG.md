# Changelog

All notable changes to this project are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [1.1.1] - 2026-05-07

### Added

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

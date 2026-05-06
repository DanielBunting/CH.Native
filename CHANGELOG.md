# Changelog

All notable changes to this project are documented in this file.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

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

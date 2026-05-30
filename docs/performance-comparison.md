# Performance Comparison

Full benchmark matrix comparing **CH.Native** against [ClickHouse.Driver](https://github.com/ClickHouse/clickhouse-cs) (HTTP) and [Octonica.ClickHouseClient](https://github.com/Octonica/ClickHouseClient) (native TCP). Each row shows **time / memory** (median wall-clock from BenchmarkDotNet, allocated bytes from `MemoryDiagnoser`). **Bold = best in row** for that metric (time and memory are bolded independently; ties bold both).

## Test conditions

| Item | Value |
|---|---|
| CPU | Apple M5 (10 logical / 10 physical cores) |
| OS | macOS 26.4.1 (Darwin 25.4.0) |
| .NET | 10.0.1 (SDK 10.0.101), Arm64 RyuJIT AdvSIMD |
| ClickHouse | `clickhouse/clickhouse-server:latest` — `26.5.1` at time of measurement |
| `ClickHouse.Driver` | `1.2.0` (HTTP, port 8123) |
| `Octonica.ClickHouseClient` | `3.1.8` (native TCP, port 9000) |
| Dapper | `2.1.35` |
| BDN config | `InvocationCount=1`, `IterationCount=10`, `WarmupCount=3`, `UnrollFactor=1` |

## Simple queries (`simple`)

| Workload | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| `SELECT 1` | 687 μs / **13 KB** | 707 μs / 537 KB | **665 μs** / 23 KB |
| `SELECT count() FROM <1M>` | 815 μs / **13 KB** | 898 μs / 546 KB | **759 μs** / 21 KB |
| `SELECT 100 rows` | 1,125 μs / **20 KB** | 944 μs / 549 KB | **832 μs** / 31 KB |

## Connection establishment (`connection`)

| Workload | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| `Connection Open` | 451 μs / 10 KB | **10 μs / 9 KB** | 1,494 μs / 23 KB |
| `Connection + Query` | **946 μs / 20 KB** | 1,029 μs / 559 KB | 1,459 μs / 61 KB |
| `10 Sequential Queries` | **5,126 μs / 94 KB** | 5,989 μs / 5,363 KB | (not measured) |

## Streaming reads — `id` only (`large`)

Reading only `id` (long); other columns transit the wire but are not accessed.

| Rows | CH.Native (Lazy) | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|---|
| 10K | 5 ms / **13 KB** | 5 ms / 1 MB | 12 ms / 2 MB | **2 ms** / 0.8 MB |
| 100K | 15 ms / **24 KB** | 27 ms / 10 MB | 108 ms / 20 MB | **10 ms** / 8 MB |
| 1M | **79 ms** / **0.13 MB** | 115 ms / 101 MB | 262 ms / 195 MB | **79 ms** / 80 MB |

## Streaming reads — `id + name (string)` (`large`)

Reading both an integer and a string column; both materialised per row.

| Rows | CH.Native (Lazy) | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|---|
| 10K | 3 ms / 549 KB | 3 ms / 556 KB | 5 ms / 1.3 MB | **2 ms / 0.9 MB** |
| 100K | 28 ms / **5.4 MB** | 19 ms / 8 MB | 26 ms / 8 MB | **10 ms** / 9 MB |
| 1M | **58 ms / 54 MB** | 76 ms / **54 MB** | 86 ms / 78 MB | 73 ms / 91 MB |

## Materialised typed reads (`large`)

POCO mapping via the typed `QueryStreamAsync<T>` (CH.Native / Octonica) or `DbDataReader`-driven mappers (Driver).

| Rows | CH.Native (Lazy) | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|---|
| 10K | 6 ms / **1.8 MB** | 12 ms / **1.8 MB** | 11 ms / 3.2 MB | **4 ms** / 2.5 MB |
| 100K | 49 ms / **17 MB** | 54 ms / **17 MB** | 51 ms / 27 MB | **29 ms** / 25 MB |
| 1M | **181 ms** / **171 MB** | 192 ms / **171 MB** | 475 ms / 265 MB | 210 ms / 251 MB |

## Dapper-shaped API — 1M rows (`dapper`)

`CH.Native.Dapper` (`QueryAsync<T>` family) vs `ClickHouse.Driver + Dapper`.

| Path | CH.Native.Dapper | Driver + Dapper |
|---|---|---|
| `QueryStreamAsync<T>` (unbuffered) | **104 ms / 101 MB** | 209 ms / 172 MB |
| `QueryAsync<T>` (buffered) | **143 ms / 117 MB** | 302 ms / 188 MB |
| `QueryAsync<T>` via `IDbConnection` (DI) | **151 ms / 117 MB** | 302 ms / 188 MB |

Reference — legacy Dapper-on-CH.Native (without `CH.Native.Dapper` fast path):

| Path | Time | Allocated |
|---|---|---|
| Dapper unbuffered over `ClickHouseConnection` | 156 ms | 304 MB |
| Dapper buffered over `ClickHouseConnection` | 282 ms | 320 MB |

## Bulk insert (`insert`)

| Rows | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| 10K | **65 ms / 139 KB** | 67 ms / 1.8 MB | (errored) |
| 100K | **75 ms / 161 KB** | 88 ms / 10 MB | (errored) |
| 1M | **99 ms / 405 KB** | 929 ms / 97 MB | (errored) |

## Complex queries (`complex`)

| Workload | CH.Native | ClickHouse.Driver | Octonica |
|---|---|---|---|
| GROUP BY aggregation | **8.0 ms / 21 KB** | 9.7 ms / 543 KB | 8.1 ms / 39 KB |
| Filtered query (WHERE) | 4.4 ms / **22 KB** | 15.0 ms / 3.0 MB | **4.2 ms** / 502 KB |
| JOIN query | 12.5 ms / **21 KB** | 12.7 ms / 541 KB | **12.4 ms** / 32 KB |
| Sorted TOP 1000 | 13.3 ms / **20 KB** | 14.5 ms / 800 KB | **12.7 ms** / 82 KB |

## Compression — 100K rows (`compression`)

| Path | Time | Allocated |
|---|---|---|
| CH.Native (Zstd) | 33 ms | 9.9 MB |
| CH.Native (LZ4) | 35 ms | 9.9 MB |
| CH.Native (no compression) | 41 ms | 9.9 MB |
| Octonica (LZ4) | **9 ms** | **7.9 MB** |
| Octonica (no compression) | 18 ms | 8.2 MB |
| Driver (HTTP) | 88 ms | 19.5 MB |

## Compression — 1M rows (`compression`)

| Path | Time | Allocated |
|---|---|---|
| CH.Native (Zstd) | 91 ms | 99 MB |
| CH.Native (LZ4) | 93 ms | 99 MB |
| CH.Native (no compression) | 102 ms | 99 MB |
| Octonica (LZ4) | 75 ms | **78 MB** |
| Octonica (no compression) | **63 ms** | 82 MB |
| Driver (HTTP) | 254 ms | 191 MB |

## Geo types — 1K rows SELECT (`geocompare`)

| Type | CH.Native | ClickHouse.Driver |
|---|---|---|
| Point | **2.6 ms / 52 KB** | 4.7 ms / 953 KB |
| Ring | **3.3 ms / 297 KB** | 13.5 ms / 6.9 MB |
| MultiPolygon (100) | **2.1 ms / 58 KB** | 3.6 ms / 1.2 MB |
| Geometry | **2.7 ms / 147 KB** | 5.8 ms / 1.7 MB |

## Geo types — 1K rows INSERT (`geocompare`)

| Type | CH.Native | ClickHouse.Driver |
|---|---|---|
| Point | **60 ms / 279 KB** | 64 ms / 981 KB |
| Ring | **61 ms / 1.7 MB** | 69 ms / 2.3 MB |
| MultiPolygon | 60 ms / **248 KB** | **59 ms** / 1.0 MB |
| Geometry | **60 ms / 441 KB** | 62 ms / 1.2 MB |

## Variant types (`variantcompare`)

| Rows | CH.Native | ClickHouse.Driver |
|---|---|---|
| 1K SELECT | **2.4 ms / 74 KB** | 2.8 ms / 582 KB |
| 10K SELECT | **3.6 ms / 543 KB** | 7.6 ms / 770 KB |
| 100K SELECT | **22 ms** / 5.2 MB | **22 ms** / **2.6 MB** |

## Reproducing

```bash
# Latency / connection
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- simple
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- connection

# Reads
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- large

# Dapper
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- dapper

# Writes
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- insert
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- schemacache

# Server-side
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- complex

# Protocol / encoding
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- compression
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- types

# Pool
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- pool

# Specialised types
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- geocompare
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- variantcompare
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- jsonquery
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- jsoninsert

# Everything
dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net10.0 -- compare
```

Reports land under `benchmarks/CH.Native.Benchmarks/BenchmarkDotNet.Artifacts/results/*-report-github.md` — those are the source of truth.

## See also

- [README — Performance](../README.md#performance)
- [Benchmark sources](../benchmarks/CH.Native.Benchmarks/Benchmarks/)
- [BenchmarkDotNet artifacts](../benchmarks/CH.Native.Benchmarks/BenchmarkDotNet.Artifacts/results/)

# CH.Native.Samples.Queries

A single console project that demonstrates every supported query path in `CH.Native`. Each path is a separate `*Sample.cs` file dispatched by name from `Program.cs` — pick one with a CLI argument, watch a complete create-table → seed → query → verify → drop-table flow against a local ClickHouse using a realistic table shape. Companion to [`CH.Native.Samples.Insert`](../CH.Native.Samples.Insert/README.md).

## Running

```bash
# List the samples:
dotnet run --project samples/CH.Native.Samples.Queries

# Run a specific sample (default connection: Host=localhost;Port=9000):
dotnet run --project samples/CH.Native.Samples.Queries -- typed

# Override the connection string with a second argument:
dotnet run --project samples/CH.Native.Samples.Queries -- typed "Host=ch.example.com;Port=9000;User=...;Password=..."
```

A local ClickHouse for testing is one Docker command away. Recent images of `clickhouse/clickhouse-server` disable the `default` user's network access unless an env var is set, so include `-e CLICKHOUSE_SKIP_USER_SETUP=1` for a frictionless local run:

```bash
docker run -d --rm -p 9000:9000 --name ch-sample \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    clickhouse/clickhouse-server
```

## Samples

| Argument | Scenario | What it shows |
|---|---|---|
| `scalar` | Order analytics — total revenue, max order, distinct customers, p95 latency | `connection.ExecuteScalarAsync<T>(sql)` across multiple return types |
| `reader` | CSV-export shape — stream a wide table row-by-row | `connection.ExecuteReaderAsync` → `ClickHouseDataReader.ReadAsync` + `GetFieldValue<T>` / `IsDBNull` / `GetTypeName` |
| `rows` | Schemaless ad-hoc reporting — column shape discovered at runtime | `connection.QueryAsync(sql)` → `IAsyncEnumerable<ClickHouseRow>`; `row["name"]` access |
| `typed` | User listing service — POCO mapping with `[ClickHouseColumn]` | `connection.QueryAsync<T>(sql)` reflection-based mapping |
| `typed-fast` | Hot-path sensor analytics — 100k rows, no boxing | `connection.QueryTypedAsync<T>(sql)` with `Stopwatch` comparison |
| `parameterized` | User-input search — `WHERE category = @cat AND price > @minPrice` | `QueryAsync<T>(sql, params)` — anonymous obj and `IDictionary<string, object?>` styles |
| `linq` | Catalog filter — Where/Select/OrderBy/Take + `.ToSql()` for inspection | `connection.Table<T>(name).Where(...).Select(...).Take(5).ToListAsync()` |
| `linq-aggregates` | Dashboard tile fan-out — twelve aggregate calls against a metrics table | `CountAsync` / `SumAsync` / `AverageAsync` / `MinAsync` / `MaxAsync` / `AnyAsync` / `AllAsync` / `FirstAsync` / `SingleAsync` |
| `linq-final` | ReplacingMergeTree current-state read | `connection.Table<T>(name).Final()` after multi-version inserts |
| `linq-sample` | Approximate analytics over 100k events + custom query id for tracing | `Table<T>(name).Sample(0.1).WithQueryId("rev-rollup-…")` |
| `adonet` | Standard-tooling-compatible read flow | `ClickHouseDbConnection` / `ClickHouseDbCommand` / `DbDataReader` with `DbParameter` |
| `dapper` | App-style read — `IN @ids` arrays, snake_case → PascalCase | `dbConnection.QueryAsync<T>(sql, params)` after `ClickHouseDapperIntegration.Register()` |
| `pooled` | Service code — 8 concurrent reads through a shared pool | `dataSource.Table<T>(name)` + rented `connection.QueryAsync<T>`; pool stats |
| `resilient` | Multi-host failover with retry policy | `ResilientConnection` over `ClickHouseConnectionSettingsBuilder.WithServers(...).WithResilience(...)` |
| `progress` | Long-running scan with progress reporting and cancellation | `IProgress<QueryProgress>` + `CancellationToken` — graceful mid-query cancel |
| `log-analytics` | Oncall log dashboard — volume by level, latency by service, error rate by hour, top slowest | `connection.QueryAsync(sql)` → `IAsyncEnumerable<ClickHouseRow>` over a partitioned `MergeTree` log table |

## Picking a path

For most reads, **`typed`** is the right answer — `await foreach (var u in connection.QueryAsync<User>(sql))` covers the everyday path. Reach for **`typed-fast`** on hot paths where boxing costs matter. Reach for **`linq`** when you want compositional query building (`.Where().OrderBy().Take()`) and free SQL inspection via `.ToSql()`. Reach for **`parameterized`** when SQL is dynamic but POCOs are not. Reach for **`reader`** or **`rows`** when result sets are large and you want bounded memory. Reach for **`adonet`** when downstream code targets `System.Data.Common` instead of CH-specific types. Reach for **`dapper`** for existing Dapper apps. Reach for **`pooled`** in service code where you don't want to manage connection lifetime. Reach for **`resilient`** for multi-replica HA setups. Reach for **`linq-final`** / **`linq-sample`** for ClickHouse-specific semantics. Reach for **`progress`** only when you genuinely need progress reporting or mid-query cancellation.

## Layout

```
samples/CH.Native.Samples.Queries/
├── CH.Native.Samples.Queries.csproj
├── Program.cs                          (dispatcher: parse args, route to the sample)
├── ScalarSample.cs
├── DataReaderSample.cs
├── RawRowsSample.cs
├── TypedSample.cs
├── TypedFastSample.cs
├── ParameterizedSample.cs
├── LinqBasicsSample.cs
├── LinqAggregatesSample.cs
├── LinqFinalSample.cs
├── LinqSampleClauseSample.cs
├── AdoNetSample.cs
├── DapperSample.cs
├── DataSourcePooledSample.cs
├── ResilientSample.cs
├── ProgressCancellationSample.cs
└── LogAnalyticsSample.cs
```

Each `*Sample.cs` is a self-contained static class: domain-appropriate POCO (`file`-scoped to the sample), a single `RunAsync(string connectionString)` entry, realistic schema, an aggregation/verification query for the result, and table cleanup in a `finally`. Drop-in copyable into your own code.

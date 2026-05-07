# CH.Native.Samples.Insert

A single console project that demonstrates every supported insert path in `CH.Native`. Each path is a separate `*Sample.cs` file dispatched by name from `Program.cs` — pick one with a CLI argument, watch a complete create-table → insert → verify → drop-table flow against a local ClickHouse using a realistic table shape.

## Running

```bash
# List the samples:
dotnet run --project samples/CH.Native.Samples.Insert

# Run a specific sample (default connection: Host=localhost;Port=9000):
dotnet run --project samples/CH.Native.Samples.Insert -- collection

# Override the connection string with a second argument:
dotnet run --project samples/CH.Native.Samples.Insert -- collection "Host=ch.example.com;Port=9000;User=...;Password=..."
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
| `single` | Audit log — admin events arriving one at a time | `connection.Table<T>(name).InsertAsync(row)`; per-event INSERT round-trip |
| `collection` | Order checkout — 10k line items in one batch | `connection.Table<T>(name).InsertAsync(IEnumerable<T>)`; partitioned MergeTree, revenue rollup |
| `async` | Paginated event ingestion — 10 pages × 1k events | `connection.Table<T>(name).InsertAsync(IAsyncEnumerable<T>)`; bounded-memory streaming |
| `oneshot` | Sensor telemetry — 100k readings into a partitioned table | `connection.BulkInsertAsync<T>(...)`; perf timing, per-sensor averages |
| `long-lived` | Hot-path log ingestion — 10 batches × 5k entries | `BulkInserter<T>` (Init / Add / Complete); amortised handshake, per-batch timing |
| `dynamic` | ETL pipeline — three flavors of POCO-less insert | `DynamicBulkInserter` one-shot, granular streaming, and pre-supplied `ColumnTypes` (skips schema probe) |
| `pooled` | Service-code metric ingest — 8 concurrent workers × 5k rows | `dataSource.Table<T>(name).InsertAsync(rows)`; pool stats after fan-out |
| `cross-db` | Orders + inventory across two databases on one connection | Qualified `db.table` inserts — typed via `Table<T>("db.table")`, dynamic via `(database, tableName)` overload |
| `sql` | One-off admin / backfill flows | `ClickHouseCommand` — parameterised single-row, multi-row inline VALUES, and `INSERT ... SELECT` table copy |

## Picking a path

For most writes, **`collection`** or **`oneshot`** is the right answer — they're the same lifecycle exposed through two equally-fine API shapes. Reach for **`long-lived`** when rows arrive over time and you want to amortise the INSERT handshake. Reach for **`pooled`** when you're in service code and don't want to manage connection lifetime. Reach for **`dynamic`** when the row shape isn't a static POCO. Reach for **`cross-db`** when one connection writes to multiple databases. Reach for **`single`** sparingly — it's correct but pays per-row overhead. Reach for **`sql`** only for non-bulk admin paths or `INSERT ... SELECT`.

## Layout

```
samples/CH.Native.Samples.Insert/
├── CH.Native.Samples.Insert.csproj
├── Program.cs                          (dispatcher: parse args, route to the sample)
├── SingleRecordSample.cs
├── CollectionSample.cs
├── AsyncStreamSample.cs
├── OneShotBulkInsertSample.cs
├── LongLivedBulkInserterSample.cs
├── DynamicBulkInsertSample.cs
├── DataSourcePooledSample.cs
├── CrossDatabaseSample.cs
└── PlainSqlInsertSample.cs
```

Each `*Sample.cs` is a self-contained static class: domain-appropriate POCO (`file`-scoped to the sample), a single `RunAsync(string connectionString)` entry, realistic schema, perf timing where useful, and an aggregation query for verification. Drop-in copyable into your own code.

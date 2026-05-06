# Connection Pooling

CH.Native ships a pooled, thread-safe connection factory — `ClickHouseDataSource` — intended to be held as a singleton for the lifetime of your application. Consumers rent a `ClickHouseConnection` per unit of work; disposing the connection returns it to the pool instead of closing the socket.

## When to use it

- **Any long-lived process serving concurrent requests** (ASP.NET, background workers, gRPC services). Use `ClickHouseDataSource`.
- **Short-lived CLI tools / one-off scripts**. Skip the pool; construct `ClickHouseConnection` directly.

The rest of this document assumes the pooled path.

## Quick start

### With `Microsoft.Extensions.DependencyInjection`

```csharp
// Program.cs
builder.Services.AddClickHouse(builder.Configuration.GetSection("ClickHouse"));
```

```jsonc
// appsettings.json
{
  "ClickHouse": {
    "Host": "clickhouse.prod",
    "Port": 9000,
    "Username": "app",
    "Password": "…",
    "Pool": {
      "MaxPoolSize": 100,
      "MinPoolSize": 5,
      "PrewarmOnStart": true
    }
  }
}
```

```csharp
// Consumer
app.MapGet("/users/{id}", async (int id, ClickHouseDataSource ds, CancellationToken ct) =>
{
    await using var conn = await ds.OpenConnectionAsync(ct);
    var row = await conn.QueryAsync<User>(
        "SELECT * FROM users WHERE id = {id:UInt32}",
        new { id },
        cancellationToken: ct).FirstAsync(ct);
    return Results.Ok(row);
});
```

### Without DI

```csharp
var ds = new ClickHouseDataSource(new ClickHouseDataSourceOptions
{
    Settings = ClickHouseConnectionSettings.Parse("Host=localhost;Port=9000;…"),
    MaxPoolSize = 100,
    MinPoolSize = 10,
    PrewarmOnStart = true,
});

await using var conn = await ds.OpenConnectionAsync();
var version = await conn.ExecuteScalarAsync<string>("SELECT version()");
```

## How the pool works

- Each `OpenConnectionAsync` acquires one permit from an internal `SemaphoreSlim` sized to `MaxPoolSize`. The permit represents the right to hold a rented connection — so at any instant, the number of busy (rented-out) connections is at most `MaxPoolSize`.
- After acquiring the permit, the pool prefers an idle connection from an internal stack (`_idle`). If none is available, it creates a fresh physical connection.
- Disposing the rented connection releases the permit *and* pushes the physical connection back onto `_idle`, waking any caller parked in `OpenConnectionAsync`.
- Idle connections are culled on next rent if they exceed `ConnectionIdleTimeout`, `ConnectionLifetime`, or fail `ValidateOnRent`. Culled connections free their `_total` slot so a fresh one can be created.
- Callers waiting on a permit fail with `TimeoutException` after `ConnectionWaitTimeout` rather than blocking indefinitely.

Key invariant: `gate permits + busy connections == MaxPoolSize`. Idle connections don't consume permits, which is what lets a returned connection unblock a parked waiter.

### Connection poison detection

When a rented connection is returned, the pool inspects `CanBePooled` on the physical connection. If `true`, the connection goes back onto `_idle`. If `false`, the connection is disposed, `TotalEvicted` increments, and the next rent opens a fresh TCP session.

`CanBePooled` returns `false` when any of the following holds:

- the connection has been disposed or closed,
- the connection's `_protocolFatal` flag is set,
- a query is still in flight (`_busy` or `_currentQueryId` set),
- a sticky role override has been applied that the pool cannot reset.

**The poison rule:** `_protocolFatal` is set only when the wire is no longer in spec — never on a server-reported error. The four call sites are:

- malformed bytes / `ClickHouseProtocolException` while reading a server message;
- a drain timeout during cancellation recovery (server failed to acknowledge a cancel within budget);
- trailing bytes after `EndOfStream` in the main read pump;
- trailing bytes after EOS, or an unknown message type, in the bulk-insert read pump.

**Corollary — server exceptions keep the connection poolable.** A well-formed `ExceptionMessage` from the server — `KILL QUERY` (the `QUERY_WAS_CANCELLED` code 394 ClickHouse returns to a killed query), a SQL error, OOM, quota breach, anything else the server can report mid-query — does **not** set `_protocolFatal`. The kill terminated the query, not the connection; the wire is in a clean state for the next message. The pool returns the connection to `_idle` and the next rent reuses it.

This is intentional. Churning a fresh socket after every server-side error would be a meaningful throughput regression on workloads that legitimately probe the database for errors (e.g., `EXISTS`-style schema checks, idempotent retries, query-killed-by-watchdog patterns).

The end-to-end contracts are pinned by tests:

- `tests/CH.Native.SystemTests/Streams/PoolDiscardOnPoisonTests` — drives each `_protocolFatal` site against a mock server and verifies the pool discards on return + opens a fresh socket on the next rent.
- `tests/CH.Native.SystemTests/Resilience/BrokenConnectionPoolReturnTests` — verifies that `KILL QUERY` and SQL errors keep the connection poolable and that the next rent reuses the same physical connection.

## Configuration reference

All knobs are on `ClickHouseDataSourceOptions` (programmatic) or the `Pool` subsection of `ClickHouseConnectionOptions` (config binding).

| Property | Default | Description |
|---|---|---|
| `MaxPoolSize` | `100` | Maximum physical connections (busy + idle). Cap on DB-side concurrency from this process. |
| `MinPoolSize` | `0` | Minimum connections to keep warm. Only populated when `PrewarmOnStart = true`. |
| `ConnectionIdleTimeout` | `5 min` | Idle connections older than this are evicted on next rent. |
| `ConnectionLifetime` | `30 min` | Maximum age of a physical connection regardless of activity. Undercut your credential rotation window. |
| `ConnectionWaitTimeout` | `30 s` | How long a caller waits for a free permit before throwing `TimeoutException`. |
| `ValidateOnRent` | `false` | When `true`, runs `SELECT 1` on every rent before handing it to the caller. |
| `PrewarmOnStart` | `false` | Fire `MinPoolSize` connection opens on construction. Useful for latency-sensitive startups. |

### Sizing `MaxPoolSize`

Size to peak **concurrent** DB operations, not peak request rate. Rule of thumb:

```
MaxPoolSize ≈ p99(concurrent-in-flight-queries) × 1.5
```

- ASP.NET service serving ~40 concurrent requests each issuing 1 query → `MaxPoolSize ≈ 60`.
- Fan-out query service issuing 20 parallel queries per request, peak 10 req/s → `MaxPoolSize ≈ 200`.
- Batch ingest with a bounded parallel pipeline → match the pipeline's degree of parallelism.

Going bigger doesn't help past the point where ClickHouse's own `max_concurrent_queries` (100 per server by default) becomes the bottleneck. It just wastes sockets and file descriptors.

### Sizing `MinPoolSize` + `PrewarmOnStart`

Set `MinPoolSize` to cover your baseline traffic so the first requests don't pay connection handshake cost:

- TCP-only handshake: ~1–5 ms on LAN.
- TLS handshake: typically 20–80 ms, depending on ciphers and RTT.

A sensible baseline: `MinPoolSize = min(10, MaxPoolSize × 0.1)`, with `PrewarmOnStart = true`. Skip it for short-lived jobs.

### `ConnectionLifetime` and rotating credentials

If you use `IClickHouseJwtProvider` (or any rotating-credential provider), `ConnectionLifetime` **must** be shorter than your token TTL. The provider is invoked when the pool creates a new physical connection, not per query — so an old connection keeps using its original token until it's recycled. Set `ConnectionLifetime` to ~80% of the token TTL:

| Token TTL | Recommended `ConnectionLifetime` |
|---|---|
| 15 min | `00:12:00` |
| 1 h | `00:50:00` |
| 12 h | `10:00:00` |

For static-password setups the default `30 min` is fine; raising it reduces churn.

### When to enable `ValidateOnRent`

Default (`false`) is correct for most setups — the first query on a dead socket will fail naturally and the caller's retry logic handles it. Enable only when:

- You're behind aggressive NAT with a short idle timeout (≤60 s) that silently drops connections.
- Your callers don't retry and you can afford ~1 ms per rent.

Alternative: lower `ConnectionIdleTimeout` below the NAT timeout.

## Multi-server / keyed pools

Each keyed DataSource is an independent pool. Use keyed registrations for primary/replica splits or cross-region fanout:

```csharp
services.AddClickHouse("primary", config.GetSection("ClickHouse:Primary"));
services.AddClickHouse("replica", config.GetSection("ClickHouse:Replica"))
    .WithPool(o => o.MaxPoolSize = 50); // replicas serve fewer concurrent reads
```

```csharp
app.MapGet("/events", async (
    [FromKeyedServices("replica")] ClickHouseDataSource ds,
    CancellationToken ct) =>
{
    await using var conn = await ds.OpenConnectionAsync(ct);
    return Results.Ok(await conn.QueryAsync<Event>("SELECT * FROM events LIMIT 100", cancellationToken: ct));
});
```

## Observability

`ClickHouseDataSource.GetStatistics()` returns a snapshot:

```csharp
app.MapGet("/diag/pool", (ClickHouseDataSource ds) => Results.Ok(ds.GetStatistics()));
```

```jsonc
{
  "Total": 42,              // physical connections currently in the pool (busy + idle)
  "Idle": 35,               // idle, ready to be handed out
  "Busy": 7,                // rented out right now
  "PendingWaits": 0,        // callers parked in OpenConnectionAsync
  "TotalRentsServed": 12847,
  "TotalCreated": 42,       // physical connections ever opened (creations)
  "TotalEvicted": 0         // physical connections ever closed (expiry / errors)
}
```

Interpret:

- **`PendingWaits > 0` sustainedly** → `MaxPoolSize` is too low or your DB is too slow. Raise `MaxPoolSize` or optimize the slow queries before they tie up connections.
- **`Busy` never exceeds ~`MaxPoolSize / 2`** → you're over-provisioned. Lower `MaxPoolSize` to free up sockets.
- **`TotalEvicted` climbing fast** → connections being closed frequently. Usually `ConnectionLifetime` too short, network instability, or a version mismatch dropping connections server-side.

The pool does *not* currently emit OpenTelemetry metrics for these counters — you can scrape `GetStatistics()` on a timer and push to your metrics system manually.

## Health checks

Register an ASP.NET Core health check backed by `PingAsync` (runs `SELECT 1` through a pooled connection):

```csharp
builder.Services.AddHealthChecks()
    .AddClickHouse(name: "ch-default")
    .AddClickHouse(name: "ch-replica", serviceKey: "replica", tags: new[] { "ready" });

app.MapHealthChecks("/health");
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = reg => reg.Tags.Contains("ready"),
});
```

Each health check rents from its pool, so during a pool-exhaustion incident the health check will also time out and your orchestrator can route traffic away.

## Observed throughput

Benchmark results against a local Docker ClickHouse (Apple M5, 10 cores) for `SELECT 1` rents via `Parallel.ForEachAsync`:

| `MaxPoolSize` | Parallel workers | Mean | P95 | Peak concurrent at DB |
|--:|--:|--:|--:|--:|
| 10  | 50   | 7.4 ms   | 8.2 ms   | 10 |
| 10  | 200  | 30.8 ms  | 38.6 ms  | 10 |
| 10  | 1000 | 160.8 ms | 179.8 ms | 10 |
| 50  | 1000 | 182.3 ms | 266.9 ms | 50 |
| 100 | 1000 | 122.1 ms | 142.9 ms | 100 |
| 200 | 1000 | 124.8 ms | 148.3 ms | 200 |

Takeaways:

- **Oversubscription works**. 1000 parallel workers against a 10-slot pool completes in ~160 ms (≈ 6 K rents/s) because connections are reused back-to-back; callers queue on the semaphore and the pool drains them in order.
- **Throughput saturates around `MaxPoolSize = 100`** for this workload — the server's per-query latency, not the pool, becomes the ceiling.
- **Run your own benchmark** before production sizing: WAN RTT, TLS, query complexity, and ClickHouse version all shift these numbers.

Reproduce with `dotnet run --project benchmarks/CH.Native.Benchmarks -c Release --framework net9.0 -- pool`.

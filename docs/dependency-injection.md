# Dependency Injection

`CH.Native.DependencyInjection` is a separate package that registers a pooled `ClickHouseDataSource` with `Microsoft.Extensions.DependencyInjection`, with `IConfiguration` binding, keyed multi-database setups, rotating-credential providers, and ASP.NET health checks.

## Install

```bash
dotnet add package CH.Native.DependencyInjection
```

## Registration

`AddClickHouse` has overloads for connection-string, `IConfiguration`, fluent builder, and pre-built `ClickHouseConnectionSettings` — each with a keyed variant (except `ClickHouseConnectionSettings`, which is non-keyed only). All return an `IClickHouseDataSourceBuilder` that you can chain provider / pool registrations onto.

```csharp
// 1. Connection string
services.AddClickHouse("Host=localhost;Database=default;Username=default");

// 2. Fluent builder
services.AddClickHouse(b => b
    .WithHost("clickhouse.internal")
    .WithDatabase("analytics")
    .WithCompression(true));

// 3. From IConfiguration (binds the section, including Pool subsection)
services.AddClickHouse(configuration.GetSection("ClickHouse"));

// 4. From a built ClickHouseConnectionSettings
services.AddClickHouse(settings);

// 5/6. Keyed variants — same shapes as 1/2/3, with a service key as the second arg
services.AddClickHouse("primary", configuration.GetSection("ClickHouse:Primary"));
services.AddClickHouse("replica", "Host=replica.internal;…");
services.AddClickHouse("adhoc", b => b.WithHost("localhost"));
```

`ClickHouseDataSource` is registered as a **singleton**. Open a connection per unit of work; disposing the connection returns it to the pool.

## `appsettings.json` schema

The `IConfiguration` overload binds `ClickHouseConnectionOptions`. The shape mirrors the connection-string keys (see [Configuration](configuration.md)) plus a nested `Pool` subsection:

```jsonc
{
  "ClickHouse": {
    "Host": "clickhouse.internal",
    "Port": 9000,
    "Database": "analytics",
    "Username": "reader",
    "Password": "…",
    "Compress": true,

    "Pool": {
      "MaxPoolSize": 64,
      "MinPoolSize": 4,
      "PrewarmOnStart": true,
      "ConnectionLifetime": "00:30:00",
      "ConnectionIdleTimeout": "00:05:00",
      "ConnectionWaitTimeout": "00:00:30",
      "ValidateOnRent": false
    }
  }
}
```

Keyed setups use either flat colon-keyed names or sibling sections — the sample uses flat keys:

```jsonc
{
  "ClickHouse": { "Host": "…" },
  "ClickHouse:Primary": { "Host": "…", "AuthMethod": "Jwt", "Pool": { "ConnectionLifetime": "00:05:00" } },
  "ClickHouse:Replica": { "Host": "…" }
}
```

```csharp
services.AddClickHouse(configuration.GetSection("ClickHouse"));
services.AddClickHouse("primary", configuration.GetSection("ClickHouse:Primary"));
services.AddClickHouse("replica", configuration.GetSection("ClickHouse:Replica"));
```

## Resolving and using

```csharp
public class EventsHandler(ClickHouseDataSource dataSource)
{
    public async Task<long> CountAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<long>(
            "SELECT count() FROM events", cancellationToken: ct);
    }
}
```

For keyed registrations, use `[FromKeyedServices]`:

```csharp
app.MapGet("/events", async (
    [FromKeyedServices("replica")] ClickHouseDataSource replica,
    CancellationToken ct) =>
{
    await using var conn = await replica.OpenConnectionAsync(ct);
    return Results.Ok(await conn.QueryAsync<Event>(
        "SELECT * FROM events LIMIT 100", cancellationToken: ct).ToListAsync(ct));
});
```

## Bulk insert from the pool

`ClickHouseDataSource.CreateBulkInserterAsync<T>` rents a connection and gives you back a `BulkInserter<T>` whose disposal returns the connection:

```csharp
await using var inserter = await dataSource.CreateBulkInserterAsync<EventRow>(
    "events", cancellationToken: ct);
await inserter.InitAsync(ct);
await inserter.AddRangeStreamingAsync(rows, ct);
await inserter.CompleteAsync(ct);
```

## Rotating credentials

Plug in providers for short-lived credentials. Each new physical connection re-resolves the credential:

```csharp
services
    .AddClickHouse("primary", configuration.GetSection("ClickHouse:Primary"))
    .WithJwtProvider<MyJwtProvider>();

public sealed class MyJwtProvider(IAccessTokenService tokens) : IClickHouseJwtProvider
{
    public ValueTask<string> GetTokenAsync(CancellationToken ct)
        => tokens.GetClickHouseTokenAsync(ct);
}
```

Equivalent provider interfaces:

| Interface | Method | Method on builder |
|---|---|---|
| `IClickHouseJwtProvider` | `GetTokenAsync` | `.WithJwtProvider<T>()` |
| `IClickHousePasswordProvider` | `GetPasswordAsync` | `.WithPasswordProvider<T>()` |
| `IClickHouseSshKeyProvider` | `GetKeyAsync` (returns `SshKeyMaterial`) | `.WithSshKeyProvider<T>()` |
| `IClickHouseCertificateProvider` | `GetCertificateAsync` | `.WithCertificateProvider<T>()` |

Each method also has a factory overload that takes `Func<IServiceProvider, Func<CancellationToken, ValueTask<T>>>` if you'd rather not register a class.

### Invocation cadence

- **Once per physical connection.** The provider is invoked when the pool builds a fresh socket — cold start, post-discard rent, or post-eviction rent. Subsequent queries on the same rented connection re-use the credential resolved at open-time.
- **Staleness bound: `ConnectionLifetime`** (default 30 min). Set this at or below your token TTL so the pool recycles connections — and refreshes credentials — before they expire.
- **Failure-driven refresh is *not* guaranteed.** Server-side SQL errors keep the connection in the pool, so the credential is reused. Only protocol-fatal errors and force-disposed connections trip the discard path that causes the next rent to re-query the provider. Don't design rotation around "any failure forces a refresh" — rotate by lifetime instead.

Provider implementations should handle their own caching internally (e.g. `Azure.Identity`'s `TokenCredential` caches under the hood). The pool will call you on every physical-connection build, not just when the credential has actually changed.

## Validation

Options validation is split so that chained provider registration is allowed:

- **Shape errors fail fast at registration.** Bad pool sizes, out-of-range ports, negative timeouts — all throw inside `AddClickHouse(IConfiguration)` before it returns.
- **Auth-pairing errors surface at first DataSource resolution.** `AuthMethod=Jwt` without a `JwtToken` *or* a chained `WithJwtProvider<>()`, and `AuthMethod=SshKey` without an `SshPrivateKeyPath` *or* `WithSshKeyProvider<>()`, throw the first time something resolves `ClickHouseDataSource`.
- **`.ValidateOnStart()`** moves auth-pairing checks to `Host.StartAsync()` for fail-fast at startup:

  ```csharp
  services
      .AddClickHouse(configuration.GetSection("ClickHouse"))
      .WithJwtProvider<MyJwtProvider>()
      .ValidateOnStart();
  ```

## Health checks

```csharp
services.AddHealthChecks()
    .AddClickHouse(name: "ch-default")
    .AddClickHouse(name: "ch-primary", serviceKey: "primary", tags: new[] { "ready" })
    .AddClickHouse(name: "ch-replica", serviceKey: "replica", tags: new[] { "ready" });

app.MapHealthChecks("/health");
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = reg => reg.Tags.Contains("ready"),
});
```

Each health check rents from its pool and runs `SELECT 1`, so during a pool-exhaustion incident the health check times out and your orchestrator can route traffic away.

The `AddClickHouse` health-check overload also accepts an optional `timeout` and `tags` parameter; defaults are no per-check timeout and no tags.

## See also

- [Connection Pooling](connection-pooling.md) — how the pool works under the hood
- [Authentication](authentication.md) — when each auth method (and provider) applies
- [Configuration](configuration.md) — connection-string keys
- `samples/CH.Native.Samples.DependencyInjection` — full ASP.NET demo with all six registration shapes

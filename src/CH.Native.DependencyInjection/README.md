# CH.Native.DependencyInjection

`Microsoft.Extensions.DependencyInjection` integration for [CH.Native](https://www.nuget.org/packages/CH.Native), the high-performance .NET client for ClickHouse using the native binary TCP protocol.

Registers `ClickHouseDataSource` as a singleton with optional keyed multi-database setups, `IConfiguration` binding, rotating-credential providers, and ASP.NET health checks.

## Install

```bash
dotnet add package CH.Native.DependencyInjection
```

## Usage

### Connection string

```csharp
builder.Services.AddClickHouse("Host=localhost;Database=default;Username=default");
```

### Fluent builder

```csharp
builder.Services.AddClickHouse(b => b
    .WithHost("clickhouse.internal")
    .WithDatabase("analytics")
    .WithCompression(true)
    .WithCompressionMethod(CompressionMethod.LZ4));
```

### From `IConfiguration`

```jsonc
// appsettings.json
{
  "ClickHouse": {
    "Host": "clickhouse.internal",
    "Database": "analytics",
    "Username": "reader",
    "Pool": {
      "MaxPoolSize": 64,
      "PrewarmOnStart": true
    }
  }
}
```

```csharp
builder.Services.AddClickHouse(builder.Configuration.GetSection("ClickHouse"));
```

### Resolve and use

`ClickHouseDataSource` is a singleton; open a connection per unit of work. The connection returns to the pool on `DisposeAsync`.

```csharp
public class EventsController(ClickHouseDataSource dataSource) : ControllerBase
{
    [HttpGet]
    public async IAsyncEnumerable<Event> GetEvents()
    {
        await using var conn = await dataSource.OpenConnectionAsync();
        await foreach (var row in conn.QueryAsync<Event>("SELECT * FROM events LIMIT 1000"))
            yield return row;
    }
}
```

## Multiple databases (keyed)

```csharp
builder.Services.AddClickHouse("analytics", analyticsConnString);
builder.Services.AddClickHouse("logs", logsConnString);

public class Reporter([FromKeyedServices("analytics")] ClickHouseDataSource ds) { }
```

## Rotating credentials

Plug in providers for short-lived credentials (JWT, SSH key, mTLS cert, password). Each new physical connection re-resolves the credential.

```csharp
builder.Services
    .AddClickHouse("Host=clickhouse.internal;Database=analytics")
    .WithJwtProvider<MyJwtProvider>();

public sealed class MyJwtProvider(IAccessTokenService tokens) : IClickHouseJwtProvider
{
    public ValueTask<string> GetTokenAsync(CancellationToken ct) => tokens.GetClickHouseTokenAsync(ct);
}
```

Equivalent interfaces exist for password (`IClickHousePasswordProvider`), SSH key (`IClickHouseSshKeyProvider`), and mTLS client certificate (`IClickHouseCertificateProvider`).

### Invocation cadence

The contract is deliberately tied to physical connections, not queries or failures:

- **Once per physical connection.** The provider is invoked when the pool builds a fresh socket — cold start, post-discard rent, or post-eviction rent. Subsequent queries on the same rented connection re-use the credential resolved at open-time; the provider is *not* re-queried per query.
- **Staleness bound: `ConnectionLifetime`** (default 30 min). This is the upper bound on how long the pool will keep using a credential resolved by a previous provider call. Set `ConnectionLifetime` to match or undercut your token TTL so the pool recycles connections — and refreshes credentials — before they expire.
- **Failure-driven refresh is *not* guaranteed.** A query failure may or may not cause the provider to be re-invoked on the next rent, depending on whether the pool discards the connection. Failures that trip the discard path (protocol-fatal errors, `KILL QUERY` on an in-flight query, force-disposed connections) cause the next rent to build a fresh socket and re-query the provider. Failures that leave the connection structurally healthy (server-side SQL errors, transient cancellations) keep the connection in the pool and the credential is reused. Do not design rotation around "any failure forces a refresh".
- **Rotate by lifetime, not by retry.** If you need faster credential rotation, lower `ConnectionLifetime`. Worst case: a connection rented just before a token rotation may keep using the old token for up to `ConnectionLifetime` before the pool recycles it.

Provider implementations should handle their own caching/refresh internally (e.g. `Azure.Identity`'s `TokenCredential` caches under the hood) — the pool will call you on every physical-connection build, not just when the credential has actually changed.

### Validation cadence

Options validation is split so that chained provider registration is allowed:

- **Shape errors fail fast at registration time.** Bad pool sizes (`MaxPoolSize < 1`, `MinPoolSize > MaxPoolSize`), out-of-range ports, negative timeouts — all throw inside `AddClickHouse(IConfiguration)` before it returns.
- **Auth-pairing errors surface at first DataSource resolution.** `AuthMethod=Jwt` without a `JwtToken` *or* a chained `WithJwtProvider<>()`, and `AuthMethod=SshKey` without a `SshPrivateKeyPath` *or* `WithSshKeyProvider<>()`, throw the first time something resolves `ClickHouseDataSource`. This lets the chained provider call satisfy the requirement instead of being a false-positive registration-time error.
- **`ValidateOnStart()` opt-in for fail-fast at host startup.** Apps that want the auth-pairing check to fail during `Host.StartAsync()` rather than at the first request can chain `.ValidateOnStart()`:

  ```csharp
  builder.Services
      .AddClickHouse(builder.Configuration.GetSection("ClickHouse"))
      .WithJwtProvider<MyJwtProvider>()
      .ValidateOnStart();
  ```

  Internally this registers a small `IHostedService` that resolves the DataSource in `StartAsync`, triggering the deferred validator.

## Health checks

```csharp
builder.Services.AddHealthChecks()
    .AddClickHouse(name: "clickhouse");
```

## License

MIT

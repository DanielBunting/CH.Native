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

## Health checks

```csharp
builder.Services.AddHealthChecks()
    .AddClickHouse(name: "clickhouse");
```

## License

MIT

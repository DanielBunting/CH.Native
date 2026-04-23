// CH.Native DependencyInjection sample — end-to-end ASP.NET demonstration of:
//   1. Static password auth bound from IConfiguration (the 80% case).
//   2. Rotating JWT via IClickHouseJwtProvider on a keyed "primary" DataSource.
//   3. Static password on a keyed "replica" DataSource.
//   4. mTLS via IClickHouseCertificateProvider on a keyed "mtls" DataSource.
//   5. SSH-key auth via IClickHouseSshKeyProvider on a keyed "ssh" DataSource.
//   6. Programmatic (no config) registration on a keyed "adhoc" DataSource.
//   7. Per-DataSource ASP.NET health checks.
//   8. BulkInserter<T> rented from the pool.
//
// Run locally against a ClickHouse container:
//   docker run --rm -p 9000:9000 -p 8123:8123 -p 9440:9440 clickhouse/clickhouse-server
//   dotnet run --project samples/CH.Native.Samples.DependencyInjection
//
// The keyed "mtls" / "ssh" / "primary" endpoints will fail handshake on a vanilla
// OSS container — they're wired up for shape, not for actually passing. The
// default DataSource + "replica" + "adhoc" work end-to-end.

using System.Security.Cryptography.X509Certificates;
using CH.Native.Connection;
using CH.Native.DependencyInjection;
using CH.Native.DependencyInjection.HealthChecks;
using CH.Native.Mapping;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);

// 1) Default DataSource — static password from appsettings.json:"ClickHouse".
builder.Services.AddClickHouse(builder.Configuration.GetSection("ClickHouse"));

// 2) Keyed "primary" — rotating JWT. The provider is invoked per physical
//    connection (not per query), gated by the pool's ConnectionLifetime.
builder.Services
    .AddClickHouse("primary", builder.Configuration.GetSection("ClickHouse:Primary"))
    .WithJwtProvider<DemoJwtProvider>();

// 3) Keyed "replica" — static password (no provider needed).
builder.Services.AddClickHouse("replica", builder.Configuration.GetSection("ClickHouse:Replica"));

// 4) Keyed "mtls" — cert pulled from a provider (e.g. reading from X509Store).
builder.Services
    .AddClickHouse("mtls", builder.Configuration.GetSection("ClickHouse:Mtls"))
    .WithCertificateProvider<DemoCertificateProvider>();

// 5) Keyed "ssh" — key material from a vault-like provider.
builder.Services
    .AddClickHouse("ssh", builder.Configuration.GetSection("ClickHouse:Ssh"))
    .WithSshKeyProvider<DemoSshKeyProvider>();

// 6) Keyed "adhoc" — purely programmatic, no IConfiguration involvement.
builder.Services
    .AddClickHouse("adhoc", b => b
        .WithHost("localhost")
        .WithDatabase("default")
        .WithUsername("default"))
    .WithPool(p =>
    {
        // Shrink pool for a short-lived sample process; real apps would leave defaults.
        // Record properties are init-only, so this mutator is illustrative — in
        // production use IConfiguration's Pool section instead.
    });

// 7) Health checks — one per DataSource. Each wires to the pool's PingAsync.
builder.Services.AddHealthChecks()
    .AddClickHouse(name: "ch-default")
    .AddClickHouse(name: "ch-primary", serviceKey: "primary", tags: new[] { "ready" })
    .AddClickHouse(name: "ch-replica", serviceKey: "replica", tags: new[] { "ready" });

var app = builder.Build();

app.MapHealthChecks("/health");
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = reg => reg.Tags.Contains("ready"),
});

// ---------------------------------------------------------------------------
// 8) Endpoints — every rent shape.
// ---------------------------------------------------------------------------

app.MapGet("/", () => "CH.Native DI sample — see /events/count, /replica/server, /diag/pool, /health.");

app.MapGet("/events/count", async (ClickHouseDataSource ds, CancellationToken ct) =>
{
    await using var conn = await ds.OpenConnectionAsync(ct);
    // Sample query — against a vanilla container this returns zero rows by using
    // a system table that always exists.
    var count = await conn.ExecuteScalarAsync<ulong>("SELECT count() FROM system.numbers LIMIT 10", ct);
    return Results.Ok(new { count });
});

app.MapGet("/replica/server", async (
    [FromKeyedServices("replica")] ClickHouseDataSource replica,
    CancellationToken ct) =>
{
    await using var conn = await replica.OpenConnectionAsync(ct);
    var version = await conn.ExecuteScalarAsync<string>("SELECT version()", ct);
    return Results.Ok(new { version });
});

app.MapPost("/events/bulk", async (
    ClickHouseDataSource ds,
    IEnumerable<EventRow> rows,
    CancellationToken ct) =>
{
    // Ensure a demo table exists.
    await using (var setup = await ds.OpenConnectionAsync(ct))
    {
        await setup.ExecuteNonQueryAsync(
            "CREATE TABLE IF NOT EXISTS sample_events (event_id UUID, ts DateTime, payload String) ENGINE = Memory", ct);
    }

    await using var inserter = await ds.CreateBulkInserterAsync<EventRow>("sample_events", cancellationToken: ct);
    await inserter.InitAsync(ct);
    var total = 0;
    foreach (var row in rows)
    {
        await inserter.AddAsync(row, ct);
        total++;
    }
    await inserter.CompleteAsync(ct);
    return Results.Accepted(value: new { inserted = total });
});

app.MapGet("/diag/pool", (ClickHouseDataSource ds) => Results.Ok(ds.GetStatistics()));

app.MapGet("/ping/{key}", async (string key, IServiceProvider sp, CancellationToken ct) =>
{
    var ds = sp.GetRequiredKeyedService<ClickHouseDataSource>(key);
    return Results.Ok(new { key, healthy = await ds.PingAsync(ct) });
});

app.Run();

// ---------------------------------------------------------------------------
// Provider implementations. Stubs so the sample compiles without external SDKs;
// real apps would plug in Azure.Identity, AWS SecretsManager, HashiCorp Vault,
// the Windows cert store, etc.
// ---------------------------------------------------------------------------

public sealed class DemoJwtProvider : IClickHouseJwtProvider
{
    // In real code: Azure.Identity's TokenCredential, Auth0, Okta, etc.
    public ValueTask<string> GetTokenAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult("demo.jwt.token");
}

public sealed class DemoCertificateProvider : IClickHouseCertificateProvider
{
    // In real code: X509Store lookup by thumbprint, file on disk, Key Vault, etc.
    public ValueTask<X509Certificate2> GetCertificateAsync(CancellationToken cancellationToken)
    {
        // A demo in-memory self-signed cert so the sample doesn't need a cert file.
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new CertificateRequest("CN=CH.Native.Sample", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256, System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        return ValueTask.FromResult(cert);
    }
}

public sealed class DemoSshKeyProvider : IClickHouseSshKeyProvider
{
    // In real code: read from a secrets store. Not a valid key — stub for shape.
    public ValueTask<SshKeyMaterial> GetKeyAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult(new SshKeyMaterial(PrivateKey: new byte[] { 0x30, 0x82 }, Passphrase: null));
}

public sealed class EventRow
{
    [ClickHouseColumn(Name = "event_id")] public Guid Id { get; set; }
    [ClickHouseColumn(Name = "ts")] public DateTime Timestamp { get; set; }
    [ClickHouseColumn(Name = "payload")] public string? Payload { get; set; }
}

using System.Data;
using System.Data.Common;
using System.Reflection;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using CH.Native.Connection;
// CH.Native.Dapper intentionally NOT imported — `using Dapper;` below brings
// Dapper's IDbConnection extensions into scope, and importing CH.Native.Dapper
// here would create ambiguity with our IDbConnectionDapperExtensions. The only
// call from CH.Native.Dapper in this file is the Register() one-shot below,
// which is fully qualified.
using CH.Native.DependencyInjection;
using CH.Native.SystemTests.Fixtures;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace CH.Native.SystemTests.DependencyInjection;

/// <summary>
/// Pins the architectural gap between the two parallel connection types in
/// CH.Native. The <see cref="ClickHouseConnection"/> returned by
/// <see cref="ClickHouseDataSource.OpenConnectionAsync"/> (the only thing DI
/// registers) is the native-protocol type — it is <c>sealed</c> and does not
/// derive from <see cref="DbConnection"/>, so Dapper's
/// <see cref="IDbConnection"/>-bound extension methods do not bind. Dapper
/// users today must drop out of DI entirely and instantiate
/// <see cref="ClickHouseConnection"/> directly from a connection string,
/// losing pooling, credential providers, keyed services, and resilience
/// configuration.
///
/// <para>
/// Every test in Groups B-K below is a <b>breaking</b> test: it fails today,
/// passes once the gap is closed. The reflection-driven failure messages are
/// deliberate — they describe the missing API surface and so double as the
/// acceptance criteria for whatever fix is chosen (see the plan file for
/// Options A/B/C). Group A is two baseline tests that pin existing behaviour
/// so the fix doesn't regress what works.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.DependencyInjection)]
public sealed class DapperDataSourceIntegrationGapsTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    static DapperDataSourceIntegrationGapsTests()
    {
        // Process-global; idempotent. Required so any Dapper QueryAsync<T>
        // calls below honour CH.Native's array handlers and snake_case
        // matching — without it the Dapper-path baseline would diverge
        // from the typed-row-mapper path.
        CH.Native.Dapper.ClickHouseDapperIntegration.Register();
    }

    public DapperDataSourceIntegrationGapsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    // ---------- Shared bootstrap & reflection helpers ----------

    private ServiceProvider BuildServices(Action<IClickHouseDataSourceBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        var builder = services.AddClickHouse(b => b
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, _fx.Password));
        configure?.Invoke(builder);
        return services.BuildServiceProvider();
    }

    private ServiceProvider BuildKeyedServices(string primaryKey, string secondaryKey)
    {
        var services = new ServiceCollection();
        services.AddClickHouse(primaryKey, b => b
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, _fx.Password));
        services.AddClickHouse(secondaryKey, b => b
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, _fx.Password));
        return services.BuildServiceProvider();
    }

    private static MethodInfo? FindOpenDbConnectionMethod() =>
        typeof(ClickHouseDataSource)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m =>
            {
                var rt = m.ReturnType;
                if (!rt.IsGenericType) return false;
                var def = rt.GetGenericTypeDefinition();
                if (def != typeof(Task<>) && def != typeof(ValueTask<>)) return false;
                return typeof(DbConnection).IsAssignableFrom(rt.GetGenericArguments()[0]);
            });

    private static MethodInfo? FindCreateDbConnectionMethod() =>
        typeof(ClickHouseDataSource)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m => typeof(DbConnection).IsAssignableFrom(m.ReturnType));

    private static string DataSourceMethodSummary() =>
        string.Join(", ",
            typeof(ClickHouseDataSource)
                .GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.DeclaredOnly)
                .Select(m => $"{m.Name}:{Format(m.ReturnType)}"));

    private static string Format(Type t) =>
        t.IsGenericType
            ? $"{t.Name[..t.Name.IndexOf('`')]}<{string.Join(",", t.GetGenericArguments().Select(Format))}>"
            : t.Name;

    /// <summary>
    /// Attempts to open a Dapper-compatible <see cref="DbConnection"/> via
    /// whatever public DataSource method returns one. Returns <c>null</c> if
    /// no such method exists — every breaking E2E test below converts that
    /// null into an <see cref="XunitException"/> with the gap described.
    /// </summary>
    private static async Task<DbConnection?> TryOpenDbConnectionAsync(
        ClickHouseDataSource ds, CancellationToken ct = default)
    {
        var method = FindOpenDbConnectionMethod();
        if (method is null) return null;

        var args = method.GetParameters().Length == 1 ? new object?[] { ct } : Array.Empty<object?>();
        var result = method.Invoke(ds, args);
        return result switch
        {
            Task<DbConnection> t => await t.ConfigureAwait(false),
            ValueTask<DbConnection> vt => await vt.ConfigureAwait(false),
            _ => await CastAsync(result!).ConfigureAwait(false),
        };

        // Awaits a Task<X> / ValueTask<X> where X derives from DbConnection
        // (handles the case where the method's declared return type is the
        // concrete ClickHouseConnection, not the DbConnection base).
        static async Task<DbConnection?> CastAsync(object task)
        {
            if (task is Task tsk)
            {
                await tsk.ConfigureAwait(false);
                var resultProp = task.GetType().GetProperty("Result");
                return resultProp?.GetValue(task) as DbConnection;
            }
            // ValueTask<X> — convert via AsTask().
            var asTask = task.GetType().GetMethod("AsTask")?.Invoke(task, null) as Task;
            if (asTask is null) return null;
            await asTask.ConfigureAwait(false);
            var rp = asTask.GetType().GetProperty("Result");
            return rp?.GetValue(asTask) as DbConnection;
        }
    }

    private static XunitException GapException(string what) =>
        new($"GAP: {what}\n\n" +
            $"ClickHouseDataSource's current public surface (declared methods): {DataSourceMethodSummary()}");

    // =========================================================================
    // GROUP A — Baseline regression guards (must keep passing)
    // =========================================================================

    [Fact]
    public async Task Baseline_DirectClickHouseConnection_Dapper_Works()
    {
        // The only Dapper path that exists today: instantiate the ADO wrapper
        // directly from a connection string. No pool, no DI, no providers.
        await using var conn = new ClickHouseConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        var n = (await conn.QueryAsync<long>("SELECT toInt64(1)")).Single();
        Assert.Equal(1L, n);
    }

    [Fact]
    public async Task Baseline_DataSource_NativeApi_Works()
    {
        // The only DI path that exists today: resolve the DataSource and use
        // the native-protocol API. Pool, providers, keyed services all work
        // here — but Dapper extension methods do not bind to the returned type.
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        await using var conn = await ds.OpenConnectionAsync();
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    // =========================================================================
    // GROUP B — Type & interface satisfaction
    // =========================================================================

    [Fact]
    public async Task Breaking_ClickHouseConnection_ImplementsIDbConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        await using var conn = await ds.OpenConnectionAsync();

        // Reflection-based check (vs `is IDbConnection`) because the compiler knows the
        // sealed return type isn't IDbConnection and would short-circuit the pattern
        // match to a dead-code warning. The assertion intent is identical.
        Assert.True(typeof(IDbConnection).IsInstanceOfType(conn),
            "ClickHouseConnection (returned from ClickHouseDataSource.OpenConnectionAsync) " +
            "does not implement IDbConnection. Dapper's extension methods are defined as " +
            "`this IDbConnection` and will not bind to the DI-supplied connection.");
    }

    [Fact]
    public async Task Breaking_ClickHouseConnection_ImplementsDbConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        await using var conn = await ds.OpenConnectionAsync();

        Assert.True(typeof(DbConnection).IsInstanceOfType(conn),
            "ClickHouseConnection is not a DbConnection subclass. Frameworks that depend on the " +
            "ADO.NET base type (EF Core's IDbConnectionFactory, OpenTelemetry SqlClient " +
            "instrumentation, Dapper transaction helpers, etc.) cannot consume it.");
    }

    [Fact]
    public void Breaking_ClickHouseDataSource_DerivesFromDbDataSource()
    {
        Assert.True(typeof(DbDataSource).IsAssignableFrom(typeof(ClickHouseDataSource)),
            "ClickHouseDataSource does not derive from System.Data.Common.DbDataSource. " +
            ".NET 8+ idiom (Npgsql, MySqlConnector, SqlClient) is to register the DataSource " +
            "as a DbDataSource so consumers inject the ADO base and call CreateConnection() " +
            "for Dapper/ADO usage. Without this, `[FromServices] DbDataSource` won't resolve.");
    }

    [Fact]
    public void Breaking_DbProviderFactory_Singleton_IsRegistered()
    {
        using var sp = BuildServices();
        var factory = sp.GetService<DbProviderFactory>();
        Assert.NotNull(factory);
        Assert.IsType<ClickHouseProviderFactory>(factory);
    }

    // =========================================================================
    // GROUP C — DI resolution for Dapper-compatible types
    // =========================================================================

    [Fact]
    public void Breaking_DI_Resolves_DbConnection()
    {
        using var sp = BuildServices();
        var conn = sp.GetService<DbConnection>();
        Assert.NotNull(conn);
    }

    [Fact]
    public void Breaking_DI_Resolves_IDbConnection()
    {
        using var sp = BuildServices();
        var conn = sp.GetService<IDbConnection>();
        Assert.NotNull(conn);
    }

    [Fact]
    public void Breaking_DI_Resolves_ClickHouseConnection()
    {
        using var sp = BuildServices();
        var conn = sp.GetService<ClickHouseConnection>();
        Assert.NotNull(conn);
    }

    [Fact]
    public void Breaking_DI_Resolves_DbDataSource()
    {
        using var sp = BuildServices();
        var ds = sp.GetService<DbDataSource>();
        Assert.NotNull(ds);
    }

    [Fact]
    public void Breaking_DI_GetRequiredService_DbConnection_DoesNotThrow()
    {
        using var sp = BuildServices();
        var ex = Record.Exception(() => sp.GetRequiredService<DbConnection>());
        Assert.Null(ex);
    }

    [Fact]
    public void Breaking_DI_Resolves_FuncOfDbConnection()
    {
        // Many codebases inject a factory delegate rather than the connection
        // itself so each unit of work owns disposal. None of those patterns
        // work today because no DbConnection-shaped service is registered.
        using var sp = BuildServices();
        var factory = sp.GetService<Func<DbConnection>>();
        Assert.NotNull(factory);
    }

    // =========================================================================
    // GROUP D — DataSource ADO API surface
    // =========================================================================

    [Fact]
    public void Breaking_DataSource_HasOpenDbConnectionAsync()
    {
        var method = FindOpenDbConnectionMethod();
        Assert.True(method is not null,
            "ClickHouseDataSource exposes no public method returning Task<DbConnection> / " +
            "ValueTask<DbConnection> (or a DbConnection-derived generic). Dapper cannot " +
            $"consume a pooled connection from DI today. Public methods: {DataSourceMethodSummary()}");
    }

    [Fact]
    public async Task Breaking_DataSource_OpenDbConnectionAsync_ReturnsOpenConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection.");

        await using (conn)
        {
            Assert.Equal(ConnectionState.Open, conn.State);
        }
    }

    [Fact]
    public async Task Breaking_DataSource_OpenDbConnectionAsync_HonoursCancellation()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var method = FindOpenDbConnectionMethod()
            ?? throw GapException("DataSource has no API returning a DbConnection.");

        // Method without a CancellationToken parameter cannot honour cancellation.
        Assert.True(
            method.GetParameters().Length == 1
                && method.GetParameters()[0].ParameterType == typeof(CancellationToken),
            "Future OpenDbConnectionAsync must accept a CancellationToken so request-scoped " +
            "cancellation reaches the pool.");

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await using var _ = await TryOpenDbConnectionAsync(ds, cts.Token).ConfigureAwait(false);
        });
    }

    [Fact]
    public void Breaking_DataSource_HasCreateDbConnection_Sync()
    {
        // DbDataSource.CreateConnection() (sync, returns an un-opened DbConnection)
        // is the most common ADO consumer entry point — Dapper transaction helpers,
        // EF Core's pooled-DbContext flow, healthchecks. Today no such method
        // exists on ClickHouseDataSource.
        var method = FindCreateDbConnectionMethod();
        Assert.True(method is not null,
            "ClickHouseDataSource exposes no public method returning DbConnection synchronously. " +
            "Sync ADO consumers cannot bridge into the pool. " +
            $"Public methods: {DataSourceMethodSummary()}");
    }

    // =========================================================================
    // GROUP E — End-to-end Dapper on a DI-sourced connection
    // =========================================================================

    [Fact]
    public async Task Breaking_Dapper_QueryAsync_OnDataSourceConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — Dapper.QueryAsync cannot be invoked.");

        await using (conn)
        {
            var rows = (await conn.QueryAsync<long>("SELECT toInt64(number) FROM numbers(3)")).ToList();
            Assert.Equal(new long[] { 0, 1, 2 }, rows);
        }
    }

    [Fact]
    public async Task Breaking_Dapper_ExecuteScalarAsync_OnDataSourceConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — Dapper.ExecuteScalarAsync cannot be invoked.");

        await using (conn)
        {
            var n = await conn.ExecuteScalarAsync<long>("SELECT toInt64(42)");
            Assert.Equal(42L, n);
        }
    }

    [Fact]
    public async Task Breaking_Dapper_ExecuteAsync_OnDataSourceConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — Dapper.ExecuteAsync cannot be invoked.");

        var table = $"gap_exec_{Guid.NewGuid():N}";
        await using (conn)
        {
            try
            {
                await conn.ExecuteAsync($"CREATE TABLE {table} (id UInt32, name String) ENGINE = Memory");
                var rows = (await conn.QueryAsync<string>(
                    "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = @t",
                    new { t = table })).ToList();
                Assert.Single(rows);
            }
            finally
            {
                await conn.ExecuteAsync($"DROP TABLE IF EXISTS {table}");
            }
        }
    }

    [Fact]
    public async Task Breaking_Dapper_WithParameters_OnDataSourceConnection()
    {
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — parameter-bound Dapper queries cannot run.");

        await using (conn)
        {
            var rows = (await conn.QueryAsync<long>(
                "SELECT toInt64(number) FROM numbers(@count) WHERE number >= @min",
                new { count = 10, min = 7 })).ToList();
            Assert.Equal(new long[] { 7, 8, 9 }, rows);
        }
    }

    [Fact]
    public async Task Breaking_Dapper_QueryMultipleAsync_OnDataSourceConnection()
    {
        // ClickHouse has no multiple-result-set / multi-statement concept, so
        // Dapper's grid-reader pattern can never work. The CH.Native.Dapper
        // QueryMultipleAsync extension surfaces that as an immediate, actionable
        // NotSupportedException rather than letting the raw multi-statement SQL
        // reach the server and fail with the opaque "error 62 (Syntax error:
        // Multi-statements are not allowed)". (Called fully-qualified so it binds
        // to our extension and not Dapper's IDbConnection overload, which is also
        // in scope via `using Dapper;`.)
        await using var sp = BuildServices();
        var ds = sp.GetRequiredService<ClickHouseDataSource>();
        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — QueryMultipleAsync cannot be invoked.");

        await using (conn)
        {
            var ex = await Assert.ThrowsAsync<NotSupportedException>(
                () => CH.Native.Dapper.IDbConnectionDapperExtensions.QueryMultipleAsync(
                    conn, "SELECT toInt64(1); SELECT toInt64(2);"));

            Assert.Contains("multiple result sets", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    // =========================================================================
    // GROUP F — Pool integration through the ADO path
    // =========================================================================

    [Fact]
    public async Task Breaking_DataSource_DbConnectionDispose_ReturnsToPool()
    {
        // Pool sized to 1: if disposing the ADO wrapper truly returns the
        // underlying native connection to the pool, the second rent must
        // succeed quickly. If it leaks (no return path), the second rent
        // either blocks until the wait-timeout or opens a side-channel
        // socket — both of which are wrong.
        await using var sp = BuildServices(b => b.WithPool(o =>
        {
            o.MaxPoolSize = 1;
            o.ConnectionWaitTimeout = TimeSpan.FromSeconds(2);
        }));
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        var first = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection — pool-return-on-dispose cannot be exercised.");
        await first.DisposeAsync();

        await using var second = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException("DataSource has no API returning a DbConnection.");
        Assert.Equal(ConnectionState.Open, second.State);
        Assert.Equal(1L, (await second.QueryAsync<long>("SELECT toInt64(1)")).Single());
    }

    [Fact]
    public async Task Breaking_DataSource_DbConnection_BlocksOnPoolSaturation()
    {
        // Saturate the pool via the native API, then attempt to open via the
        // ADO API. The ADO path must contend with the same pool — not open
        // a side-channel socket. Today: no ADO open API exists, so this
        // documents the gap. Once implemented, a regression where the ADO
        // path bypasses the pool will fail this assertion.
        await using var sp = BuildServices(b => b.WithPool(o =>
        {
            o.MaxPoolSize = 1;
            o.ConnectionWaitTimeout = TimeSpan.FromMilliseconds(500);
        }));
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        await using var nativeHolder = await ds.OpenConnectionAsync();

        var method = FindOpenDbConnectionMethod()
            ?? throw GapException("DataSource has no API returning a DbConnection — pool-saturation gating on the ADO path cannot be exercised.");

        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await using var _ = await TryOpenDbConnectionAsync(ds).ConfigureAwait(false);
        });
    }

    // =========================================================================
    // GROUP G — Credential providers reaching the Dapper path
    // =========================================================================

    [Fact]
    public async Task Breaking_DataSource_DbConnection_InvokesPasswordProvider()
    {
        // The password provider is the workhorse because the bundled
        // SingleNodeFixture handshake actually accepts password auth —
        // matches the pattern from JwtProviderInvocationCadenceTests.
        var invocations = 0;
        var realPassword = _fx.Password;
        await using var sp = BuildServices(b => b.WithPasswordProvider(_ => _ =>
        {
            Interlocked.Increment(ref invocations);
            return new ValueTask<string>(realPassword);
        }));
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        var conn = await TryOpenDbConnectionAsync(ds)
            ?? throw GapException(
                "DataSource has no API returning a DbConnection — credentials registered via " +
                ".WithPasswordProvider() cannot reach the Dapper/ADO path. " +
                "Today the only way to get an IDbConnection is to call `new ClickHouseConnection(connStr)` " +
                "directly, which knows nothing about the DI graph.");

        await using (conn)
        {
            Assert.Equal(1, Volatile.Read(ref invocations));
            Assert.Equal(1L, (await conn.QueryAsync<long>("SELECT toInt64(1)")).Single());
        }
    }

    [Fact]
    public async Task Breaking_DataSource_DbConnection_InvokesJwtProvider_Smoke()
    {
        // JWT smoke: the 24.8 image does not accept JWT auth, so the
        // handshake itself will fail — but the provider must still be queried
        // before that failure. Mirrors the smoke in
        // JwtProviderInvocationCadenceTests but on the ADO path.
        var invocations = 0;
        await using var sp = BuildServices(b => b.WithJwtProvider(_ => _ =>
        {
            Interlocked.Increment(ref invocations);
            return new ValueTask<string>("not-a-real-jwt");
        }));
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        try
        {
            await using var conn = await TryOpenDbConnectionAsync(ds)
                ?? throw GapException(
                    "DataSource has no API returning a DbConnection — JWT provider cannot reach the Dapper/ADO path.");
        }
        catch (XunitException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _output.WriteLine($"Expected handshake failure: {ex.GetType().Name}");
        }

        Assert.True(invocations >= 1,
            "JWT provider should have been invoked at least once on Dapper-path connection open.");
    }

    [Fact]
    public async Task DirectClickHouseConnection_DoesNotConsultDIProvider_UseDataSourceInstead()
    {
        // Documents the deliberate seam (Option B): credential providers are a
        // pooled-DataSource feature, matching Npgsql — a hand-constructed
        // `new ClickHouseConnection(connStr)` reads only the static credentials
        // from its connection string and knows nothing about the DI graph.
        // Rotation comes from the DataSource's open paths, not the bare ctor.
        // One counting provider, one container: the direct ctor must leave it at
        // zero; the pooled rent must drive it above zero.
        var invocations = 0;
        var realPassword = _fx.Password;

        await using var sp = BuildServices(b => b.WithPasswordProvider(_ => _ =>
        {
            Interlocked.Increment(ref invocations);
            return new ValueTask<string>(realPassword);
        }));

        // Force the DI graph to materialise so the provider factory is wired up.
        var ds = sp.GetRequiredService<ClickHouseDataSource>();

        // --- Direct ctor: provider must NOT be consulted (the documented limitation). ---
        await using (var direct = new ClickHouseConnection(_fx.ConnectionString))
        {
            await direct.OpenAsync();
            _ = (await direct.QueryAsync<long>("SELECT toInt64(1)")).Single();
        }
        Assert.Equal(0, Volatile.Read(ref invocations));

        // --- DataSource pooled path: same provider IS consulted (the supported path). ---
        await using (var pooled = await ds.OpenConnectionAsync())
        {
            Assert.Equal(1L, (await pooled.QueryAsync<long>("SELECT toInt64(1)")).Single());
        }

        Assert.True(Volatile.Read(ref invocations) >= 1,
            "Credential rotation is a DataSource feature: ClickHouseDataSource.OpenConnectionAsync() " +
            "(and OpenDbConnectionAsync() for the ADO/Dapper path) must consult the registered " +
            "provider. Direct `new ClickHouseConnection(connStr)` intentionally does not — use the " +
            "DataSource open paths when credentials rotate.");
    }

    // =========================================================================
    // GROUP H — Keyed DI
    // =========================================================================

    [Fact]
    public void Breaking_KeyedDataSource_ResolvesDbConnectionByKey()
    {
        using var sp = BuildKeyedServices("primary", "secondary");
        var primary = sp.GetKeyedService<DbConnection>("primary");
        Assert.NotNull(primary);
    }

    [Fact]
    public async Task Breaking_KeyedDataSource_OpenDbConnectionAsync_PerKey()
    {
        await using var sp = BuildKeyedServices("primary", "secondary");
        var primary = sp.GetRequiredKeyedService<ClickHouseDataSource>("primary");
        var secondary = sp.GetRequiredKeyedService<ClickHouseDataSource>("secondary");

        var p = await TryOpenDbConnectionAsync(primary)
            ?? throw GapException("Keyed DataSource has no API returning a DbConnection.");
        var s = await TryOpenDbConnectionAsync(secondary)
            ?? throw GapException("Keyed DataSource has no API returning a DbConnection.");

        await using (p)
        await using (s)
        {
            // Qualified to disambiguate from CH.Native.Dapper.IDbConnectionDapperExtensions,
            // which is now in scope via the project ref. The test asserts behaviour of
            // Dapper's classic path against a DataSource-opened DbConnection.
            Assert.Equal(1L, (await p.QueryAsync<long>("SELECT toInt64(1)")).Single());
            Assert.Equal(2L, (await s.QueryAsync<long>("SELECT toInt64(2)")).Single());
        }
    }

    // =========================================================================
    // GROUP I — Resilience policies on the Dapper path (reflection variant)
    // =========================================================================

    [Fact]
    public void Breaking_ClickHouseConnection_HasResilienceIntegration()
    {
        // Today ClickHouseConnection is a thin ADO wrapper around an inner
        // ClickHouseConnection that it opens fresh on OpenAsync — it does not
        // participate in any retry / circuit-breaker policy that was attached
        // to the DataSource. Reflect on the type for any obvious resilience
        // hook (a Resilience-named member, a RetryPolicy field, etc.).
        var hits = typeof(ClickHouseConnection)
            .GetMembers(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(m => m.Name.Contains("Retry", StringComparison.OrdinalIgnoreCase)
                     || m.Name.Contains("CircuitBreaker", StringComparison.OrdinalIgnoreCase)
                     || m.Name.Contains("Resilience", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.True(hits.Count > 0,
            "ClickHouseConnection has no member referencing Retry / CircuitBreaker / Resilience. " +
            "Resilience policies configured on the DI builder cannot reach the Dapper path because " +
            "OpenAsync builds a fresh inner ClickHouseConnection from the connection string and " +
            "bypasses the DataSource pool entirely.");
    }

    [Fact]
    public void Breaking_DataSource_ExposesResilienceConfig_ForAdoWrapper()
    {
        // The fix needs a way for an ADO connection produced by the DataSource
        // to inherit the same retry/circuit-breaker policies as a pooled
        // native rent. Reflect on the DataSource for a Resilience-shaped
        // accessor — none today.
        var resilienceish = typeof(ClickHouseDataSource)
            .GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m.Name.Contains("Retry", StringComparison.OrdinalIgnoreCase)
                     || m.Name.Contains("CircuitBreaker", StringComparison.OrdinalIgnoreCase)
                     || m.Name.Contains("Resilience", StringComparison.OrdinalIgnoreCase))
            .ToList();

        Assert.True(resilienceish.Count > 0,
            "ClickHouseDataSource exposes no resilience accessor; even if an ADO open path is added, " +
            "the wrapper has no way to discover the configured retry/circuit-breaker policy.");
    }

    // =========================================================================
    // GROUP J — Bridging gaps in ClickHouseConnection
    //
    // The previous "Breaking_ClickHouseConnection_HasPublicConstructor_FromClickHouseConnection"
    // and "Breaking_ClickHouseConnection_Inner_IsPublic" gap tests were resolved
    // by collapsing the ClickHouseDbConnection / ClickHouseConnection pair into
    // a single ClickHouseConnection. There is no wrapper to construct around a
    // rent and no Inner to escape to — the rent IS the native connection IS the
    // ADO connection. The pin below documents that resolution.
    // =========================================================================

    [Fact]
    public void GapClosed_ConnectionTypesAreUnified()
    {
        // After Phase-2 + the ADO collapse, there is a single sealed
        // ClickHouseConnection that satisfies both the native API and DbConnection
        // contracts. No wrapper, no Inner, no second connection type.
        Assert.True(typeof(System.Data.Common.DbConnection).IsAssignableFrom(typeof(ClickHouseConnection)),
            "ClickHouseConnection no longer inherits DbConnection. ADO consumers cannot bind.");
        Assert.True(typeof(ClickHouseConnection).IsSealed,
            "ClickHouseConnection is no longer sealed — the unification contract is at risk.");
        Assert.Null(typeof(ClickHouseConnection)
            .GetProperty("Inner", BindingFlags.Public | BindingFlags.Instance));
    }

    // =========================================================================
    // GROUP K — Shape parity between the two connection types
    // =========================================================================

    [Fact]
    public void Breaking_ClickHouseConnection_HasCreateCommandReturningDbCommand()
    {
        var method = typeof(ClickHouseConnection)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m => m.Name == "CreateCommand"
                                 && typeof(DbCommand).IsAssignableFrom(m.ReturnType)
                                 && m.GetParameters().Length == 0);
        Assert.True(method is not null,
            "ClickHouseConnection has no public CreateCommand() returning DbCommand. ADO consumers " +
            "that hold a reference to a pooled native connection cannot create a portable command.");
    }

    [Fact]
    public void Breaking_ClickHouseConnection_HasBeginTransactionReturningDbTransaction()
    {
        var method = typeof(ClickHouseConnection)
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m => m.Name == "BeginTransaction"
                                 && typeof(DbTransaction).IsAssignableFrom(m.ReturnType));
        Assert.True(method is not null,
            "ClickHouseConnection has no public BeginTransaction(): DbTransaction. The ADO wrapper " +
            "throws NotSupportedException for transactions — the contract should be identical on both " +
            "layers (both expose the method; both throw the same exception).");
    }
}

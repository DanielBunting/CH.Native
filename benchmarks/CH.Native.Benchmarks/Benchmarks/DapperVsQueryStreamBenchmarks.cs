using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using Dapper;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using NativeAdoConnection = CH.Native.Ado.ClickHouseDbConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;
// CH.Native.Dapper is NOT imported wholesale because the existing Driver
// benchmarks call SqlMapper extensions on IDbConnection — even after dropping
// the row-shaped IDbConnection extensions from CH.Native.Dapper, qualifying
// the concrete-type extension classes keeps the call sites unambiguous and
// easy to read.
using ChNativeDapperAdo = CH.Native.Dapper.ClickHouseDbConnectionDapperExtensions;
using ChNativeDapperNative = CH.Native.Dapper.ClickHouseConnectionDapperExtensions;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Compares typed row materialisation across:
///   - CH.Native's native streaming path (<c>QueryStreamAsync&lt;T&gt;</c>)
///   - Dapper over CH.Native's ADO.NET layer (buffered and unbuffered)
///   - Dapper over ClickHouse.Driver's ADO.NET layer (buffered and unbuffered)
///
/// Targets the existing benchmark tables: 100 rows (<c>SimpleTable</c>) and
/// 1M rows (<c>LargeTable</c>). The <c>LargeTable</c> schema differs slightly
/// from <c>SimpleTable</c>, so each path projects only the four common columns.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
[MemoryDiagnoser]
public class DapperVsQueryStreamBenchmarks
{
    private NativeConnection _nativeConnection = null!;
    private NativeAdoConnection _nativeAdoConnection = null!;
    private DriverConnection _driverConnection = null!;

    private const string SmallSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.SimpleTable;

    private const string LargeSql =
        "SELECT id, name, value, created FROM " + TestDataGenerator.LargeTable;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        await TestDataGenerator.EnsureTablesCreatedAsync();

        CH.Native.Dapper.ClickHouseDapperIntegration.Register();

        var manager = BenchmarkContainerManager.Instance;

        _nativeConnection = new NativeConnection(manager.NativeConnectionString);
        await _nativeConnection.OpenAsync();

        _nativeAdoConnection = new NativeAdoConnection(manager.NativeConnectionString);
        await _nativeAdoConnection.OpenAsync();

        _driverConnection = new DriverConnection(manager.DriverConnectionString);
        await _driverConnection.OpenAsync();
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _nativeConnection.DisposeAsync();
        await _nativeAdoConnection.DisposeAsync();
        await _driverConnection.DisposeAsync();
    }

    // --- 100 rows (small result set) ---

    [Benchmark(Description = "100 rows - Native QueryStreamAsync<T>")]
    public async Task<int> Native_QueryStream_Small()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryStreamAsync<SimpleRow>(SmallSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "100 rows - Native Dapper QueryAsync (buffered)")]
    public async Task<int> NativeDapper_QueryAsync_Buffered_Small()
    {
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(SmallSql);
        return rows.Count();
    }

    [Benchmark(Description = "100 rows - Native Dapper QueryAsync (unbuffered)")]
    public async Task<int> NativeDapper_QueryAsync_Unbuffered_Small()
    {
        int count = 0;
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(SmallSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "100 rows - Driver Dapper QueryAsync (buffered)")]
    public async Task<int> DriverDapper_QueryAsync_Buffered_Small()
    {
        var rows = await _driverConnection.QueryAsync<SimpleRow>(SmallSql);
        return rows.Count();
    }

    [Benchmark(Description = "100 rows - Driver Dapper QueryAsync (unbuffered)")]
    public async Task<int> DriverDapper_QueryAsync_Unbuffered_Small()
    {
        int count = 0;
        var rows = await _driverConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(SmallSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    // --- 1M rows (large result set) ---

    [Benchmark(Description = "1M rows - Native QueryStreamAsync<T>")]
    public async Task<int> Native_QueryStream_Large()
    {
        int count = 0;
        await foreach (var row in _nativeConnection.QueryStreamAsync<SimpleRow>(LargeSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "1M rows - Native Dapper QueryAsync (buffered)")]
    public async Task<int> NativeDapper_QueryAsync_Buffered_Large()
    {
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(LargeSql);
        return rows.Count();
    }

    [Benchmark(Description = "1M rows - Native Dapper QueryAsync (unbuffered)")]
    public async Task<int> NativeDapper_QueryAsync_Unbuffered_Large()
    {
        int count = 0;
        var rows = await _nativeAdoConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(LargeSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "1M rows - Driver Dapper QueryAsync (buffered)")]
    public async Task<int> DriverDapper_QueryAsync_Buffered_Large()
    {
        var rows = await _driverConnection.QueryAsync<SimpleRow>(LargeSql);
        return rows.Count();
    }

    [Benchmark(Description = "1M rows - Driver Dapper QueryAsync (unbuffered)")]
    public async Task<int> DriverDapper_QueryAsync_Unbuffered_Large()
    {
        int count = 0;
        var rows = await _driverConnection.QueryAsync<SimpleRow>(
            new CommandDefinition(LargeSql, flags: CommandFlags.None));
        foreach (var _ in rows)
        {
            count++;
        }
        return count;
    }

    // Diagnostic: bypass Dapper entirely and exercise IsDBNull + GetXxx on
    // ClickHouseDbDataReader directly. Confirms whether per-row boxing is on
    // our side or somewhere in Dapper's codegen.

    [Benchmark(Description = "1M rows - Direct IsDBNull+GetXxx (no Dapper)")]
    public async Task<long> Direct_IsDBNull_GetXxx_Large()
    {
        using var cmd = _nativeAdoConnection.CreateCommand();
        cmd.CommandText = LargeSql;
        await using var reader = await cmd.ExecuteReaderAsync();

        long sum = 0;
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0)) sum += reader.GetInt64(0);
            if (!reader.IsDBNull(2)) sum += (long)reader.GetDouble(2);
            if (!reader.IsDBNull(3)) sum += reader.GetDateTime(3).Ticks;
        }
        return sum;
    }

    // Diagnostic: simulate Dapper's actual access pattern — `reader[i]` which
    // calls GetValue(int) and boxes value types. If this matches Dapper's
    // allocation profile, we know the indexer is the hot path.
    [Benchmark(Description = "1M rows - Direct GetValue (indexer) (no Dapper)")]
    public async Task<long> Direct_GetValue_Large()
    {
        using var cmd = _nativeAdoConnection.CreateCommand();
        cmd.CommandText = LargeSql;
        await using var reader = await cmd.ExecuteReaderAsync();

        long sum = 0;
        while (await reader.ReadAsync())
        {
            var v0 = reader.GetValue(0);
            var v2 = reader.GetValue(2);
            var v3 = reader.GetValue(3);
            if (v0 is long l) sum += l;
            if (v2 is double d) sum += (long)d;
            if (v3 is DateTime dt) sum += dt.Ticks;
        }
        return sum;
    }

    // Diagnostic: hand-rolled mapper that mirrors what an optimal Dapper-style
    // mapper would do — IsDBNull + typed GetXxx per column, materialise into
    // SimpleRow, accumulate in a List<T>. If this is close to Dapper's number,
    // the gap is Dapper's own machinery (delegate dispatch, parameter array,
    // member set). If it's far below, Dapper is using a slower access pattern
    // (e.g. GetValue indexer with boxing).
    [Benchmark(Description = "1M rows - Hand-rolled mapper (typed accessors)")]
    public async Task<int> Direct_HandRolledMapper_Large()
    {
        using var cmd = _nativeAdoConnection.CreateCommand();
        cmd.CommandText = LargeSql;
        await using var reader = await cmd.ExecuteReaderAsync();

        var list = new List<SimpleRow>(capacity: 1_048_576);
        while (await reader.ReadAsync())
        {
            list.Add(new SimpleRow
            {
                Id = reader.IsDBNull(0) ? 0L : reader.GetInt64(0),
                Name = reader.IsDBNull(1) ? "" : reader.GetString(1),
                Value = reader.IsDBNull(2) ? 0d : reader.GetDouble(2),
                Created = reader.IsDBNull(3) ? default : reader.GetDateTime(3),
            });
        }
        return list.Count;
    }

    // ----------------------------------------------------------------------
    // CH.Native.Dapper fast-path benchmarks (the deliverable of this work).
    // Same Dapper-style call shape, but routed through CH.Native's typed
    // mapper. Expect allocations to match the hand-rolled-typed-accessor
    // floor (~172 MB / 1M rows) instead of the boxing-tax ~304 MB.
    // ----------------------------------------------------------------------

    // --- 100 rows ---

    [Benchmark(Description = "100 rows - CH.Native.Dapper QueryAsync<T> (typed conn)")]
    public async Task<int> ChDapper_QueryAsync_Typed_Small()
    {
        var rows = await ChNativeDapperAdo.QueryAsync<SimpleRow>(_nativeAdoConnection, SmallSql);
        return rows.Count;
    }

    [Benchmark(Description = "100 rows - CH.Native.Dapper QueryStreamAsync<T> (typed conn)")]
    public async Task<int> ChDapper_QueryStream_Typed_Small()
    {
        int count = 0;
        await foreach (var _ in ChNativeDapperAdo.QueryStreamAsync<SimpleRow>(_nativeAdoConnection, SmallSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "100 rows - CH.Native.Dapper QueryAsync<T> (native conn, DI shape)")]
    public async Task<int> ChDapper_QueryAsync_NativeConn_Small()
    {
        // The sibling fast path on ClickHouseConnection — the type returned by
        // ClickHouseDataSource.OpenConnectionAsync. Equivalent to writing
        // `connection.QueryAsync<SimpleRow>(sql)` with `using CH.Native.Dapper;`.
        var rows = await ChNativeDapperNative.QueryAsync<SimpleRow>(_nativeConnection, SmallSql);
        return rows.Count;
    }

    [Benchmark(Description = "100 rows - Dapper QueryAsync<T> on native conn (IDbConnection fallback)")]
    public async Task<int> Dapper_QueryAsync_OnNativeConn_Small()
    {
        // Baseline: what users get today when the variable is typed as
        // ClickHouseConnection (DI shape) and only `using Dapper;` is in scope —
        // Dapper's IDbConnection extension wins, mapping goes through Dapper's
        // codegen with the boxing tax.
        System.Data.IDbConnection conn = _nativeConnection;
        var rows = await conn.QueryAsync<SimpleRow>(SmallSql);
        return rows.Count();
    }

    // --- 1M rows ---

    [Benchmark(Description = "1M rows - CH.Native.Dapper QueryAsync<T> (typed conn)")]
    public async Task<int> ChDapper_QueryAsync_Typed_Large()
    {
        var rows = await ChNativeDapperAdo.QueryAsync<SimpleRow>(_nativeAdoConnection, LargeSql);
        return rows.Count;
    }

    [Benchmark(Description = "1M rows - CH.Native.Dapper QueryStreamAsync<T> (typed conn)")]
    public async Task<int> ChDapper_QueryStream_Typed_Large()
    {
        int count = 0;
        await foreach (var _ in ChNativeDapperAdo.QueryStreamAsync<SimpleRow>(_nativeAdoConnection, LargeSql))
        {
            count++;
        }
        return count;
    }

    [Benchmark(Description = "1M rows - CH.Native.Dapper QueryAsync<T> (native conn, DI shape)")]
    public async Task<int> ChDapper_QueryAsync_NativeConn_Large()
    {
        var rows = await ChNativeDapperNative.QueryAsync<SimpleRow>(_nativeConnection, LargeSql);
        return rows.Count;
    }

    [Benchmark(Description = "1M rows - Dapper QueryAsync<T> on native conn (IDbConnection fallback)")]
    public async Task<int> Dapper_QueryAsync_OnNativeConn_Large()
    {
        System.Data.IDbConnection conn = _nativeConnection;
        var rows = await conn.QueryAsync<SimpleRow>(LargeSql);
        return rows.Count();
    }

    // Diagnostic: hand-rolled mapper that mirrors what Dapper's compiled
    // delegate actually emits — GetValue(i) + DBNull/null check + cast. If this
    // matches Dapper's allocation profile, we've confirmed Dapper bypasses the
    // typed accessors and routes everything through GetValue (boxing).
    [Benchmark(Description = "1M rows - Hand-rolled mapper (GetValue + cast)")]
    public async Task<int> Direct_HandRolledMapper_GetValue_Large()
    {
        using var cmd = _nativeAdoConnection.CreateCommand();
        cmd.CommandText = LargeSql;
        await using var reader = await cmd.ExecuteReaderAsync();

        var list = new List<SimpleRow>(capacity: 1_048_576);
        while (await reader.ReadAsync())
        {
            var v0 = reader.GetValue(0);
            var v1 = reader.GetValue(1);
            var v2 = reader.GetValue(2);
            var v3 = reader.GetValue(3);
            list.Add(new SimpleRow
            {
                Id = v0 is long id ? id : 0L,
                Name = v1 as string ?? "",
                Value = v2 is double val ? val : 0d,
                Created = v3 is DateTime dt ? dt : default,
            });
        }
        return list.Count;
    }

    public sealed class SimpleRow
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public double Value { get; set; }
        public DateTime Created { get; set; }
    }
}

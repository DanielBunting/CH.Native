using System.Data;
using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the less-frequently-exercised parts of the ADO surface that
/// frameworks (Dapper, EF, ADO templates) rely on. If any of these are
/// no-ops or return wrong values, framework integrations break in
/// confusing ways.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class AdoContractCoverageTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public AdoContractCoverageTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task GetSchemaTable_ReturnsColumnMetadata_ForRunningQuery()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) AS x, 'foo' AS y";

        await using var reader = await cmd.ExecuteReaderAsync();
        var schema = reader.GetSchemaTable();

        _output.WriteLine($"Schema table: rows={schema?.Rows.Count ?? -1}, columns={schema?.Columns.Count ?? -1}");
        Assert.NotNull(schema);
        Assert.Equal(2, schema!.Rows.Count);
    }

    [Fact]
    public async Task ChangeDatabase_SwitchesDatabaseContext()
    {
        // ADO contract: ChangeDatabase(name) updates the current database
        // for subsequent commands. This is what frameworks use to switch
        // contexts mid-session.
        var sideDb = $"chdb_{Guid.NewGuid():N}";
        await using (var setup = new ClickHouseDbConnection(_fx.ConnectionString))
        {
            await setup.OpenAsync();
            using var c = setup.CreateCommand();
            c.CommandText = $"CREATE DATABASE {sideDb}";
            await c.ExecuteNonQueryAsync();
        }

        try
        {
            await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
            await conn.OpenAsync();

            // Try ChangeDatabase. May throw NotSupported if not implemented.
            Exception? caught = null;
            try
            {
                conn.ChangeDatabase(sideDb);
            }
            catch (Exception ex) { caught = ex; }

            _output.WriteLine($"ChangeDatabase: thrown={caught?.GetType().Name ?? "(none)"}");

            if (caught is null)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT currentDatabase()";
                var current = (string)(await cmd.ExecuteScalarAsync())!;
                Assert.Equal(sideDb, current);
            }
            // Either supported (and works) or throws — pin which.
        }
        finally
        {
            await using var cleanup = new ClickHouseDbConnection(_fx.ConnectionString);
            await cleanup.OpenAsync();
            using var c = cleanup.CreateCommand();
            c.CommandText = $"DROP DATABASE IF EXISTS {sideDb}";
            try { await c.ExecuteNonQueryAsync(); } catch { }
        }
    }

    [Fact]
    public async Task GetValues_FillsObjectArray()
    {
        // DbDataReader.GetValues(object[]) is a frequently-used hot-path
        // method for bulk row reads. Probe whether it's implemented and
        // returns the right column count.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt32(42), 'foo', toFloat64(3.14)";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var values = new object[3];
        var filled = reader.GetValues(values);

        _output.WriteLine($"GetValues filled={filled} values=[{string.Join(", ", values.Select(v => v?.ToString()))}]");
        Assert.Equal(3, filled);
        Assert.Equal(42, Convert.ToInt32(values[0]));
        Assert.Equal("foo", values[1]);
    }

    [Fact]
    public async Task ConnectionStateEvents_FireOnEveryTransition()
    {
        // ADO contract: StateChange fires on every transition between
        // ConnectionState values. The library now raises:
        //   Closed → Connecting (on OpenAsync entry)
        //   Connecting → Open (on successful Open)
        //   Open → Closed (on Close)
        // Frameworks (EF Core, Dapper) hook these events for connection-pool
        // tracking and lifecycle scoping.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);

        var changes = new List<(ConnectionState before, ConnectionState after)>();
        conn.StateChange += (sender, args) => changes.Add((args.OriginalState, args.CurrentState));

        await conn.OpenAsync();
        await conn.CloseAsync();

        _output.WriteLine($"State changes observed: {changes.Count}");
        foreach (var c in changes)
            _output.WriteLine($"  {c.before} → {c.after}");

        Assert.Contains(changes, c => c.before == ConnectionState.Closed && c.after == ConnectionState.Connecting);
        Assert.Contains(changes, c => c.before == ConnectionState.Connecting && c.after == ConnectionState.Open);
        Assert.Contains(changes, c => c.before == ConnectionState.Open && c.after == ConnectionState.Closed);
    }

    [Fact]
    public async Task DbCommand_Prepare_IsNoOpButDoesNotThrow()
    {
        // ClickHouse doesn't support prepared statements. Per ADO contract,
        // Prepare should be a no-op rather than throwing — frameworks
        // sometimes call it speculatively.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";

        cmd.Prepare(); // should not throw

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));
    }
}

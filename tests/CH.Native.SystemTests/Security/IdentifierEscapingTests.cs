using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.Mapping;
using CH.Native.Sql;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Security.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Pins the contract on identifier escaping in the LINQ provider and bulk-insert
/// column mapping. Today, <see cref="TableNameResolver.Resolve{T}"/> emits the
/// raw snake_case form and <see cref="SqlBuilder.Table"/> stores the table name
/// verbatim; <c>[ClickHouseColumn(Name = "...")]</c> values are similarly trusted
/// without escape. Only <see cref="SqlBuilder.QuoteIdentifier"/> (used for SELECT
/// aliases) goes through <see cref="ClickHouseIdentifier.Quote"/>.
///
/// <para><b>Several tests in this file are expected to fail today.</b> They drive
/// the fix toward applying <see cref="ClickHouseIdentifier.Quote"/> uniformly in
/// <see cref="SqlBuilder.Table"/>, <see cref="TableNameResolver"/>, and the LINQ
/// expression visitor's column-name emission. Leaving them red is the point —
/// they document the gap.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class IdentifierEscapingTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fixture;
    private SentinelTable _sentinel = null!;

    public IdentifierEscapingTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _sentinel = await SentinelTable.CreateAsync(() => _fixture.BuildSettings());
    }

    public Task DisposeAsync() => _sentinel.DisposeAsync().AsTask();

    // --- TableNameResolver: pin current behaviour and drive the fix. ---

    [Fact]
    public void TableNameResolver_PlainEntity_BehaviourPinned()
    {
        // Today: returns snake_case unquoted. When the fix lands, this should
        // be backtick-quoted (i.e. `users_entity`). Update the assertion then.
        var resolved = TableNameResolver.Resolve<UsersEntity>();

        Assert.Equal("users_entity", resolved);
    }

    [Fact]
    public async Task TableNameResolver_PlainEntity_ToSql_IsQuoted()
    {
        // Architectural decision: TableNameResolver.Resolve<T> stays raw; quoting
        // belongs at the SQL-emission boundary (SqlBuilder.Table). This test
        // asserts the contract at that boundary instead of at the resolver.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var sql = conn.Table<UsersEntity>().ToSql();

        Assert.Contains(" FROM `users_entity`", sql);
    }

    // --- LINQ table-name emission: assert ToSql() quotes the FROM clause. ---

    [Fact]
    public async Task LinqQuery_TableName_AppearsQuoted_InGeneratedSql_ExpectedFailing()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var sql = conn.Table<UsersEntity>().ToSql();

        // The fix is to wrap the FROM-clause identifier with ClickHouseIdentifier.Quote.
        // Today: " FROM users" — no backticks.
        Assert.Contains(" FROM `", sql);
    }

    [Fact]
    public async Task LinqQuery_ExplicitTableName_WithBacktick_RoundTripSucceeds()
    {
        // Pre-create the table with a hostile name (backtick in the identifier)
        // using a hand-quoted DDL via ClickHouseIdentifier.Quote. If the LINQ
        // path quotes properly, the round-trip works. If not, the SELECT will
        // raise a server-side syntax error.
        var hostile = $"weird`{Guid.NewGuid():N}";
        var quoted = ClickHouseIdentifier.Quote(hostile);

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {quoted} (id Int32, value String) ENGINE = MergeTree ORDER BY id");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {quoted} VALUES (1, 'ok')");

            // This is the assertion we care about: LINQ must emit a quoted FROM.
            var rows = await conn.Table<SecurityRow>(hostile).ToListAsync();

            Assert.Single(rows);
            Assert.Equal("ok", rows[0].Value);
        }
        finally
        {
            try { await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {quoted}"); } catch { }
        }
    }

    [Fact]
    public async Task LinqQuery_ExplicitTableName_WithSemicolon_NoInjection_SentinelSurvives()
    {
        // We don't pre-create a table with this name — the assertion is purely
        // that an injection payload as a table name does NOT cause the sentinel
        // to be dropped. The LINQ call is expected to raise (table doesn't exist
        // / SQL syntax error). The critical assertion is the sentinel survives.
        var injection = $"x; DROP TABLE {_sentinel.TableName}; --";

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var ex = await Record.ExceptionAsync(() =>
            conn.Table<SecurityRow>(injection).ToListAsync());

        Assert.NotNull(ex);
        Assert.True(await _sentinel.ExistsAsync(),
            $"Sentinel {_sentinel.TableName} was dropped — table-name injection breached.");
    }

    // --- Column-attribute hostile names ---

    [Fact]
    public async Task BulkInsert_HostileColumnAttribute_WithBacktick_RoundTrips()
    {
        // Create a table whose column genuinely has a backtick in the name.
        // The bulk inserter must quote both the column and the surrounding SQL
        // for this to land. Today's behaviour: schema match is case-insensitive,
        // so the match might succeed; the question is whether the generated
        // INSERT statement quotes the column name correctly.
        var tableName = $"hostile_col_{Guid.NewGuid():N}";
        var hostileColumn = "weird`column";
        var quotedColumn = ClickHouseIdentifier.Quote(hostileColumn);

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, {quotedColumn} String) ENGINE = MergeTree ORDER BY id");
        try
        {
            await using var inserter = conn.CreateBulkInserter<HostileColumnRow>(tableName);
            await inserter.InitAsync();
            await inserter.AddAsync(new HostileColumnRow { Id = 1, Value = "ok" });
            await inserter.CompleteAsync();

            var count = await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {tableName}");
            Assert.Equal(1UL, count);
        }
        finally
        {
            try { await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}"); } catch { }
        }
    }

    [Fact]
    public async Task BulkInsert_HostileColumnAttribute_WithSemicolon_NoInjection_SentinelSurvives()
    {
        // Bulk insert with an attribute whose Name is an injection payload.
        // Even if the schema match fails, the failure must NOT execute the
        // injected DROP. The sentinel must survive regardless of outcome.
        var tableName = $"hostile_col_{Guid.NewGuid():N}";

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {tableName} (id Int32, value String) ENGINE = MergeTree ORDER BY id");
        try
        {
            await using var inserter = conn.CreateBulkInserter<SemicolonInjectionRow>(tableName);
            // InitAsync may throw if it tries to match the hostile column name —
            // that's fine. The point is: no injection executes.
            var ex = await Record.ExceptionAsync(async () =>
            {
                await inserter.InitAsync();
                await inserter.AddAsync(new SemicolonInjectionRow { Id = 1, Bad = "x" });
                await inserter.CompleteAsync();
            });

            // Either the call succeeds (matching ignored the hostile name) or
            // it failed cleanly. In neither case may the sentinel be dropped.
            _ = ex;
            Assert.True(await _sentinel.ExistsAsync(),
                $"Sentinel {_sentinel.TableName} was dropped — column-attribute injection breached.");
        }
        finally
        {
            try { await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}"); } catch { }
        }
    }

    // --- Test entities ---

    private sealed class UsersEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class HostileColumnRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "weird`column", Order = 1)] public string Value { get; set; } = "";
    }

    private sealed class SemicolonInjectionRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "value\"); DROP TABLE existing; --", Order = 1)]
        public string Bad { get; set; } = "";
    }
}

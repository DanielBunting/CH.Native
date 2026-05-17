using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins the contract for "the read loop threw partway through a Data block":
/// the failing query throws the real exception, the connection is poisoned,
/// and any subsequent operation on it surfaces a clean "Connection is broken"
/// error from <c>EnterBusy</c>. Server-side errors (bad SQL, missing table)
/// must NOT be treated as poison — they advance the pipe inline before throwing.
///
/// See <c>src/CH.Native/Connection/ClickHouseConnection.cs</c>:
/// <list type="bullet">
/// <item><c>TryReadMessage</c> / <c>TryReadTypedMessage</c> catch chains.</item>
/// <item><c>EnterBusy</c> (line 200) — the gate that surfaces the broken state.</item>
/// <item>The <c>ServerMessageType.Exception</c> arms (lines 3041, 2117) — advance
/// the pipe inline so server errors stay non-fatal.</item>
/// </list>
/// </summary>
[Collection("ClickHouse")]
public class ConnectionRecoveryTests
{
    private readonly ClickHouseFixture _fixture;

    public ConnectionRecoveryTests(ClickHouseFixture fixture) => _fixture = fixture;

    // Aggregate functions whose state format the registry doesn't decode yet.
    // All four hit ColumnReaderFactory.CreateAggregateFunctionReader →
    // AggregateFunctionStateFormatRegistry.Resolve → NotSupportedException.
    // Same shape as tests/CH.Native.SystemTests/.../AggregateFunctionEdgeCaseTests.cs.
    public static IEnumerable<object[]> UnsupportedAggregateProjections() => new[]
    {
        new object[] { "uniqExactState(toUInt64(number))",            "uniqExact" },
        new object[] { "quantilesTDigestState(0.5)(toFloat64(number))","quantilesTDigest" },
        new object[] { "groupArrayState(toInt32(number))",            "groupArray" },
        new object[] { "topKState(10)(toString(number))",             "topK" },
    };

    // ───────────────────────────────────────────────────────────────────────
    // 1. Reader-factory rejection mid-block poisons the connection
    // ───────────────────────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(UnsupportedAggregateProjections))]
    public async Task UnsupportedAggregate_ExecuteScalar_PoisonsConnection(string projection, string fnHint)
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var first = await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>($"SELECT {projection} FROM numbers(10)"));
        Assert.Contains(fnHint, first.Message);

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    [Theory]
    [MemberData(nameof(UnsupportedAggregateProjections))]
    public async Task UnsupportedAggregate_QueryAsync_PoisonsConnection(string projection, string fnHint)
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var first = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync($"SELECT {projection} FROM numbers(10)")) { }
        });
        Assert.Contains(fnHint, first.Message);

        var second = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync("SELECT 1")) { }
        });
        Assert.Contains("Connection is broken", second.Message);
    }

    [Theory]
    [MemberData(nameof(UnsupportedAggregateProjections))]
    public async Task UnsupportedAggregate_ExecuteReader_PoisonsConnection(string projection, string fnHint)
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var first = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await using var reader = await conn.ExecuteReaderAsync(
                $"SELECT {projection} FROM numbers(10)");
            while (await reader.ReadAsync()) { }
        });
        Assert.Contains(fnHint, first.Message);

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    [Theory]
    [MemberData(nameof(UnsupportedAggregateProjections))]
    public async Task UnsupportedAggregate_ExecuteNonQuery_PoisonsConnection(string projection, string fnHint)
    {
        // ExecuteNonQuery still pumps the response (Data block + EndOfStream).
        // The Data-block parse hits the reader-factory rejection just like SELECT.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var first = await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteNonQueryAsync($"SELECT {projection} FROM numbers(10)"));
        Assert.Contains(fnHint, first.Message);

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    // ───────────────────────────────────────────────────────────────────────
    // 2. Different parse paths and query shapes also poison
    // ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task UnsupportedAggregate_InMultiColumnSelect_PoisonsConnection()
    {
        // The first column reads fine; the unsupported column is the second.
        // Exercises the case where Block.ReadTypedBlockWithTableName has already
        // built one reader successfully before throwing on the next.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>(
                "SELECT toUInt64(number) % 5 AS k, uniqExactState(toUInt64(number)) " +
                "FROM numbers(50) GROUP BY k"));

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    [Fact]
    public async Task UnsupportedAggregate_LargeResult_PoisonsConnection_MultiSegmentPath()
    {
        // 10k rows pushes the response past a single pipe segment on most
        // platforms, exercising the contiguous-buffer multi-segment branch at
        // ClickHouseConnection.cs:2998–3019.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>(
                "SELECT uniqExactState(toUInt64(number)) FROM numbers(10000) GROUP BY number % 1000"));

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    [Fact]
    public async Task UnsupportedAggregate_CompressedConnection_PoisonsConnection()
    {
        // Compressed Data path goes through ReadDataMessage's
        // decompressor-then-parse loop (line 3247+). The factory rejection
        // happens after decompression but inside the outer TryReadMessage
        // try block — the same catch chain.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .Build();

        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>(
                "SELECT uniqExactState(toUInt64(number)) FROM numbers(100)"));

        var second = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", second.Message);
    }

    [Fact]
    public async Task UnsupportedAggregate_FromAggregatingMv_PoisonsConnection()
    {
        // The realistic case: an AggregatingMergeTree MV with an unsupported
        // tier-2 aggregate. SELECT * touches the state column server has
        // already materialized.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        var src = $"recovery_src_{Guid.NewGuid():N}";
        var mv = $"recovery_mv_{Guid.NewGuid():N}";

        await conn.ExecuteNonQueryAsync($"CREATE TABLE {src} (id Int32, v UInt64) ENGINE=MergeTree ORDER BY id");
        await conn.ExecuteNonQueryAsync(
            $"CREATE MATERIALIZED VIEW {mv} ENGINE=AggregatingMergeTree ORDER BY id AS " +
            $"SELECT id, uniqExactState(v) AS u FROM {src} GROUP BY id");
        try
        {
            await conn.ExecuteNonQueryAsync($"INSERT INTO {src} VALUES (1, 10), (1, 20), (2, 5)");

            await Assert.ThrowsAsync<NotSupportedException>(async () =>
            {
                await foreach (var _ in conn.QueryAsync($"SELECT id, u FROM {mv}")) { }
            });

            // Connection is now poisoned — even a follow-up DROP on the SAME
            // connection must report the broken state, not silently re-parse
            // stale bytes from the failed SELECT.
            var second = await Assert.ThrowsAsync<InvalidOperationException>(
                () => conn.ExecuteNonQueryAsync("SELECT 1"));
            Assert.Contains("Connection is broken", second.Message);
        }
        finally
        {
            // Clean up with a separate connection — the original is poisoned.
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
        }
    }

    // ───────────────────────────────────────────────────────────────────────
    // 3. After poison, EVERY operation surfaces the broken state
    // ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Poisoned_ExecuteScalar_ReportsBroken()
    {
        await using var conn = await PoisonConnectionAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteScalarAsync<int>("SELECT 1"));
        Assert.Contains("Connection is broken", ex.Message);
    }

    [Fact]
    public async Task Poisoned_ExecuteNonQuery_ReportsBroken()
    {
        await using var conn = await PoisonConnectionAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteNonQueryAsync("INSERT INTO system.numbers VALUES (1)"));
        Assert.Contains("Connection is broken", ex.Message);
    }

    [Fact]
    public async Task Poisoned_QueryAsync_ReportsBroken()
    {
        await using var conn = await PoisonConnectionAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync("SELECT 1")) { }
        });
        Assert.Contains("Connection is broken", ex.Message);
    }

    [Fact]
    public async Task Poisoned_ExecuteReader_ReportsBroken()
    {
        await using var conn = await PoisonConnectionAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.ExecuteReaderAsync("SELECT 1"));
        Assert.Contains("Connection is broken", ex.Message);
    }

    [Fact]
    public async Task Poisoned_RepeatedAttempts_AllReportBroken()
    {
        // The fatal flag is sticky — three consecutive attempts all hit the
        // same EnterBusy gate. No silent recovery, no flapping between
        // exception types.
        await using var conn = await PoisonConnectionAsync();
        for (int i = 0; i < 3; i++)
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => conn.ExecuteScalarAsync<int>("SELECT 1"));
            Assert.Contains("Connection is broken", ex.Message);
        }
    }

    [Fact]
    public async Task Poisoned_ConnectionDisposesCleanly()
    {
        // The async-using disposal path must not throw even when the
        // connection is in the poisoned state.
        var conn = await PoisonConnectionAsync();
        await conn.DisposeAsync();
    }

    // ───────────────────────────────────────────────────────────────────────
    // 4. Regression guards — server-side errors must NOT poison the connection
    // ───────────────────────────────────────────────────────────────────────
    //
    // These exist because a naïve `catch (Exception) → mark fatal` would
    // catch ClickHouseServerException (it derives from ClickHouseException :
    // Exception) and treat every bad-SQL response as wire corruption. The
    // ServerMessageType.Exception arm in TryReadMessage advances the pipe
    // inline before throwing, so the wire is in a clean state — these tests
    // pin the "connection stays usable after server error" contract.

    [Fact]
    public async Task ServerError_InvalidSql_ConnectionRemainsUsable()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<ClickHouseServerException>(
            () => conn.ExecuteScalarAsync<int>("SELECT this_function_does_not_exist()"));

        // Same connection, follow-up query must succeed.
        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task ServerError_MissingTable_ConnectionRemainsUsable()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<ClickHouseServerException>(
            () => conn.ExecuteScalarAsync<int>("SELECT * FROM no_such_table_xyz"));

        Assert.Equal(1, await conn.ExecuteScalarAsync<int>("SELECT 1"));
    }

    [Fact]
    public async Task ServerError_SyntaxError_ConnectionRemainsUsable()
    {
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<ClickHouseServerException>(
            () => conn.ExecuteNonQueryAsync("SELEC malformed"));

        Assert.Equal(42, await conn.ExecuteScalarAsync<int>("SELECT 42"));
    }

    [Fact]
    public async Task ServerError_MultipleErrorsAndRecoveries_ConnectionRemainsUsable()
    {
        // Drives the recovery path repeatedly: server error, then success,
        // server error, then success, …. Each cycle must leave the
        // connection in a usable state.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        for (int i = 0; i < 5; i++)
        {
            await Assert.ThrowsAsync<ClickHouseServerException>(
                () => conn.ExecuteScalarAsync<int>($"SELECT * FROM no_table_{i}"));
            Assert.Equal(i, await conn.ExecuteScalarAsync<int>($"SELECT {i}"));
        }
    }

    [Fact]
    public async Task ServerError_FollowedByQuery_ConnectionRemainsUsable_QueryAsync()
    {
        // Same regression guard for the streaming-result entrypoint.
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        await Assert.ThrowsAsync<ClickHouseServerException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync("SELECT * FROM no_such_table")) { }
        });

        var rows = new List<int>();
        await foreach (var r in conn.QueryAsync("SELECT number FROM numbers(3)"))
            rows.Add((int)r.GetFieldValue<ulong>(0));
        Assert.Equal(new[] { 0, 1, 2 }, rows);
    }

    // ───────────────────────────────────────────────────────────────────────
    // Helper
    // ───────────────────────────────────────────────────────────────────────

    private async Task<ClickHouseConnection> PoisonConnectionAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await Assert.ThrowsAsync<NotSupportedException>(
            () => conn.ExecuteScalarAsync<object>(
                "SELECT uniqExactState(toUInt64(number)) FROM numbers(10)"));
        return conn;
    }
}

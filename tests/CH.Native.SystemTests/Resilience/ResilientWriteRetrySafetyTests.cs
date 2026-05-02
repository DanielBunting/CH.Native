using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pre-fix <see cref="ResilientConnection.ExecuteNonQueryAsync"/> and the
/// <see cref="System.Collections.Generic.IEnumerable{T}"/> overload of
/// <c>BulkInsertAsync</c> wrapped the entire operation in
/// <c>ExecuteWithResilienceAsync</c>. A transient socket reset that arrived
/// after ClickHouse had already accepted the INSERT block would be retried,
/// duplicating rows on a non-replicated MergeTree.
///
/// The contract this suite pins:
/// 1. Write SQL passed to <c>ExecuteNonQueryAsync</c> is NOT retried — the
///    transient surfaces to the caller, and the table holds at most the rows
///    from one attempt.
/// 2. <see cref="ResilientConnection"/>'s <c>IEnumerable&lt;T&gt;</c>
///    bulk-insert overload no longer retries on transients (now matches the
///    documented behaviour of the <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>
///    overload).
/// </summary>
[Collection("MultiToxiproxy")]
[Trait(Categories.Name, Categories.Resilience)]
public class ResilientWriteRetrySafetyTests
{
    private readonly MultiToxiproxyFixture _fx;
    private readonly ITestOutputHelper _output;

    public ResilientWriteRetrySafetyTests(MultiToxiproxyFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_TransientResetDuringInsert_DoesNotDuplicate()
    {
        var table = $"retry_safety_{Guid.NewGuid():N}";
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 4,
                BaseDelay = TimeSpan.FromMilliseconds(50),
            })));

        // Defensive cleanup — a previous test may have left toxics on this proxy.
        await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);

        // ClickHouseConnection (non-resilient) reads Host/Port; only ResilientConnection
        // honours the Servers list that the multi-toxiproxy fixture builds. Use the
        // resilient form for setup/verify so the fixture's proxy endpoints route correctly.
        await using var setup = new ResilientConnection(_fx.BuildSettings(new[] { _fx.EndpointA }));
        await setup.OpenAsync();
        try
        {
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32) ENGINE = MergeTree ORDER BY id");

            await using var conn = new ResilientConnection(settings);
            await conn.OpenAsync();

            // Reset the wire AFTER the bytes are on flight. Pre-fix the retry
            // policy would re-issue the same INSERT statement; post-fix the
            // exception bubbles up untouched.
            await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
                new() { ["timeout"] = 0 });

            try
            {
                await Assert.ThrowsAnyAsync<Exception>(() =>
                    conn.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (1), (2), (3)"));
            }
            finally
            {
                await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
            }

            // Whatever the server saw, the row count must be either 0 (no
            // bytes accepted) or 3 (one full batch). It must NEVER be a
            // multiple of 3, which would prove the retry duplicated.
            var count = await setup.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"row count after transient INSERT: {count}");
            Assert.True(count == 0 || count == 3,
                $"row count should be 0 or 3 (single attempt); duplicate-retry would yield 6 / 9. saw {count}.");
        }
        finally
        {
            try { await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); } catch { }
        }
    }

    [Fact]
    public async Task BulkInsertAsync_IEnumerable_TransientResetDuringInsert_DoesNotDuplicate()
    {
        var table = $"retry_bi_{Guid.NewGuid():N}";
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 4,
                BaseDelay = TimeSpan.FromMilliseconds(50),
            })));

        // Defensive cleanup — a previous test may have left toxics on this proxy.
        await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);

        // ClickHouseConnection (non-resilient) reads Host/Port; only ResilientConnection
        // honours the Servers list that the multi-toxiproxy fixture builds. Use the
        // resilient form for setup/verify so the fixture's proxy endpoints route correctly.
        await using var setup = new ResilientConnection(_fx.BuildSettings(new[] { _fx.EndpointA }));
        await setup.OpenAsync();
        try
        {
            await setup.ExecuteNonQueryAsync(
                $"CREATE TABLE {table} (id Int32) ENGINE = MergeTree ORDER BY id");

            await using var conn = new ResilientConnection(settings);
            await conn.OpenAsync();

            await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
                new() { ["timeout"] = 0 });

            var rows = Enumerable.Range(0, 1000).Select(i => new IdRow { Id = i }).ToList();
            try
            {
                await Assert.ThrowsAnyAsync<Exception>(() =>
                    conn.BulkInsertAsync(table, rows));
            }
            finally
            {
                await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
            }

            var count = await setup.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
            _output.WriteLine($"BulkInsert row count after transient: {count}");
            Assert.True(count <= 1000,
                $"Bulk insert under transient should not duplicate rows; expected ≤ 1000, saw {count}.");
        }
        finally
        {
            try { await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}"); } catch { }
        }
    }

    [Fact]
    public async Task ExecuteScalarAsync_TransientReset_StillRetriesReadOnlySql()
    {
        // Sanity: read-only statements continue to be retried — the fix only
        // suppresses retry for write SQL.
        var settings = _fx.BuildSettings(
            new[] { _fx.EndpointA },
            b => b.WithResilience(r => r.WithRetry(new RetryOptions
            {
                MaxRetries = 6,
                BaseDelay = TimeSpan.FromMilliseconds(80),
            })));

        await using var conn = new ResilientConnection(settings);
        await conn.OpenAsync();

        await _fx.Client.AddToxicAsync(_fx.ProxyAName, "reset_peer", "downstream",
            new() { ["timeout"] = 0 });
        var clearer = Task.Run(async () =>
        {
            await Task.Delay(300);
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        });

        try
        {
            var v = await conn.ExecuteScalarAsync<int>("SELECT 11");
            Assert.Equal(11, v);
        }
        finally
        {
            await clearer;
            await _fx.Client.RemoveAllToxicsAsync(_fx.ProxyAName);
        }
    }

    private sealed class IdRow
    {
        [Mapping.ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }
    }
}

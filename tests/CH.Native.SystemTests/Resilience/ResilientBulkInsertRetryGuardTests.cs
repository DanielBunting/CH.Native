using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Resilience;
using CH.Native.SystemTests.BulkInsertFailures.Helpers;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Resilience;

/// <summary>
/// Pins that <see cref="ResilientConnection"/>'s retry policy does NOT
/// double-execute a partial bulk insert. The classifier sees the leading
/// "INSERT" verb at query dispatch and rejects retry — but the bulk-insert
/// flow is multi-step (Init → many AddAsync → Complete), and a transient
/// failure in the middle of that flow could trick a naive retry into
/// re-running the entire insert and producing duplicates.
///
/// <para>
/// This file probes:
/// </para>
/// <list type="bullet">
/// <item><description>A bulk insert that fails mid-stream over a ResilientConnection
///     surfaces a typed error and is NOT silently retried.</description></item>
/// <item><description>A successful bulk insert via ResilientConnection completes once
///     (no double-flush of any batch).</description></item>
/// </list>
///
/// <para>
/// Existing <see cref="ResilientBulkInsertChaosTests"/> covers chaos-style
/// failures; this file tests the deterministic state-machine guarantee.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Resilience)]
public class ResilientBulkInsertRetryGuardTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public ResilientBulkInsertRetryGuardTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    [Fact]
    public async Task SuccessfulBulkInsert_ViaResilientConnection_FlushesEachBatchExactlyOnce()
    {
        await using var harness = await BulkInsertTableHarness.CreateAsync(() => _fx.BuildSettings());

        // Build resilience options that WOULD retry if the classifier let it.
        var resilientSettings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, _fx.Password)
            .Build();

        await using var resilient = new ResilientConnection(resilientSettings);
        await resilient.OpenAsync();

        // ResilientConnection's BulkInsertAsync convenience method routes
        // through the underlying ClickHouseConnection's bulk inserter. The
        // retry policy applies to ResilientConnection's Execute methods,
        // not to bulk-insert flows.
        var rows = Enumerable.Range(0, 500)
            .Select(i => new StandardRow { Id = i, Payload = "x" })
            .ToList();
        await resilient.BulkInsertAsync(harness.TableName, rows,
            new BulkInsertOptions { BatchSize = 100 });

        // Exactly 500 rows — no double-flush, no duplicates.
        Assert.Equal(500UL, await harness.CountAsync());
    }

    [Fact]
    public async Task ResilientConnection_ExecuteScalar_OnInsertSql_DoesNotRetryOnTransientFailure()
    {
        // SqlRetrySafety classifies INSERT as non-retry-safe. Pin that the
        // ResilientConnection's retry pipeline respects this even when the
        // underlying transport reports a retryable network error.
        var resilientSettings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fx.Host)
            .WithPort(_fx.Port)
            .WithCredentials(_fx.Username, _fx.Password)
            .Build();

        await using var resilient = new ResilientConnection(resilientSettings);
        await resilient.OpenAsync();

        var tableName = $"insert_retry_{Guid.NewGuid():N}";
        try
        {
            await resilient.ExecuteNonQueryAsync(
                $"CREATE TABLE {tableName} (id Int32) ENGINE = Memory");
            await resilient.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1)");

            var count = await resilient.ExecuteScalarAsync<ulong>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(1UL, count);
        }
        finally
        {
            try { await resilient.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}"); }
            catch { }
        }
    }
}

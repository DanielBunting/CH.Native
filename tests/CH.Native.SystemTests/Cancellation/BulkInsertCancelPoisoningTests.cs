using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Cancellation;

/// <summary>
/// Asserts the cancelled-bulk-insert recovery contract: after a cancellation,
/// the same connection must be reusable. The bulk-insert path (BulkInserter)
/// sends a Cancel packet and drains the server's response on cancellation,
/// matching the read-path behaviour exercised by
/// <c>tests/CH.Native.Tests/Integration/CancellationTests.cs</c>.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class BulkInsertCancelPoisoningTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public BulkInsertCancelPoisoningTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task SameConnection_AfterCancelledBulkInsert_IsReusable()
    {
        var table = $"bi_poison_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
            await conn.OpenAsync();

            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50)))
            {
                try
                {
                    await using var inserter = conn.CreateBulkInserter<Row>(table,
                        new BulkInsertOptions { BatchSize = 1000 });
                    await inserter.InitAsync(cts.Token);

                    var s = new string('x', 256);
                    for (int i = 0; i < 100_000 && !cts.IsCancellationRequested; i++)
                        await inserter.AddAsync(new Row { Id = i, Payload = s }, cts.Token);
                    await inserter.CompleteAsync(cts.Token);
                }
                catch (OperationCanceledException) { /* expected */ }
                catch (Exception ex)
                {
                    _output.WriteLine($"BulkInsert cancellation surfaced as: {ex.GetType().Name}: {ex.Message}");
                }
            }

            // After cancellation + drain, the same connection is reusable; SELECT 1
            // must return 1.
            int observed;
            try
            {
                observed = await conn.ExecuteScalarAsync<int>("SELECT 1");
            }
            catch (Exception ex)
            {
                _output.WriteLine($"Probe SELECT 1 threw: {ex.GetType().Name}: {ex.Message}");
                throw new Xunit.Sdk.XunitException(
                    "Connection reuse after cancelled bulk insert threw; the contract " +
                    "is that the cancel path drains and leaves the connection clean.");
            }

            _output.WriteLine($"Probe SELECT 1 returned: {observed}");
            Assert.Equal(1, observed);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task FreshConnection_AfterCancelledBulkInsert_AlwaysWorks()
    {
        // Counterpart to the pinning test above — the SERVER is not poisoned, only
        // the specific connection's wire state. A new connection always works.
        var table = $"bi_poison_fresh_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.BuildSettings());
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, payload String) ENGINE = Memory");

        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50)))
            {
                try
                {
                    await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
                    await conn.OpenAsync(cts.Token);
                    await using var inserter = conn.CreateBulkInserter<Row>(table,
                        new BulkInsertOptions { BatchSize = 1000 });
                    await inserter.InitAsync(cts.Token);
                    var s = new string('x', 256);
                    for (int i = 0; i < 100_000 && !cts.IsCancellationRequested; i++)
                        await inserter.AddAsync(new Row { Id = i, Payload = s }, cts.Token);
                    await inserter.CompleteAsync(cts.Token);
                }
                catch (Exception) { /* cancellation expected */ }
            }

            await using var fresh = new ClickHouseConnection(_fixture.BuildSettings());
            await fresh.OpenAsync();
            Assert.Equal(1, await fresh.ExecuteScalarAsync<int>("SELECT 1"));
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
    }
}

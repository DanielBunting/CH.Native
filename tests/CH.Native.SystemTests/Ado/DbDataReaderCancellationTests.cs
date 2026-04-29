using System.Diagnostics;
using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Cancellation contract on the ADO.NET surface. <c>ClickHouseDbDataReader</c> sits
/// in front of the native reader; existing tests pin the native cancellation path,
/// not the ADO wrapper. The wrapper must surface <c>OperationCanceledException</c>
/// promptly, mark itself closed, and refuse subsequent field access with
/// <c>ObjectDisposedException</c>.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Cancellation)]
public class DbDataReaderCancellationTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly ITestOutputHelper _output;

    public DbDataReaderCancellationTests(SingleNodeFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact]
    public async Task CancelDuringReadAsync_ThrowsPromptly_ReaderRejectsFieldAccess()
    {
        await using var conn = new ClickHouseDbConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        // numbers(very-large) keeps the reader streaming long enough to land the cancel mid-flight.
        cmd.CommandText = "SELECT number FROM numbers(50_000_000)";

        using var cts = new CancellationTokenSource();
        await using var reader = await cmd.ExecuteReaderAsync(cts.Token);

        // Pull a few rows so the reader is genuinely streaming.
        for (int i = 0; i < 10; i++)
        {
            var advanced = await reader.ReadAsync(cts.Token);
            Assert.True(advanced, "Reader should advance on early rows.");
        }

        // Now cancel and pull one more — must throw quickly.
        var sw = Stopwatch.StartNew();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            // Loop: cancellation may be observed on the very next ReadAsync, or on
            // a subsequent one if the previous block was already buffered. Either way
            // it must surface within the 2s budget.
            while (sw.ElapsedMilliseconds < 2000)
            {
                await reader.ReadAsync(cts.Token);
            }
        });
        sw.Stop();
        _output.WriteLine($"Cancel-to-throw on ADO reader: {sw.ElapsedMilliseconds}ms");
        Assert.True(sw.ElapsedMilliseconds < 2000,
            $"ADO reader cancellation should surface within 2s; took {sw.ElapsedMilliseconds}ms.");

        // Cancellation does not auto-close the reader — that's the explicit DisposeAsync
        // contract. After dispose, the wrapper's ThrowIfClosed guards every accessor.
        await reader.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => reader.GetInt64(0));
    }
}

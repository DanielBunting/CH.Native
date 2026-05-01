using System.Diagnostics;
using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// <c>FieldCount</c> and <c>HasRows</c> are property getters that must be cheap;
/// Dapper and other ADO consumers read them many times per row. The
/// implementation guards an inner <c>ReadAsync</c> with an <c>_initialized</c>
/// flag — this test pins that the guard prevents repeated wire calls.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Streams)]
public class DataReaderPropertyAccessTests
{
    private readonly SingleNodeFixture _fx;

    public DataReaderPropertyAccessTests(SingleNodeFixture fx) => _fx = fx;

    [Fact]
    public async Task FieldCount_RepeatedAccess_RemainsCheap()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT number, number * 2 AS doubled FROM numbers(10)";
        await using var reader = await cmd.ExecuteReaderAsync();

        // Warm: first access pays the schema-init cost (single ReadAsync).
        var initial = reader.FieldCount;
        Assert.Equal(2, initial);

        var sw = Stopwatch.StartNew();
        const int iterations = 10_000;
        for (int i = 0; i < iterations; i++)
        {
            _ = reader.FieldCount;
            _ = reader.HasRows;
        }
        sw.Stop();

        // Both getters together should average sub-microsecond per pair if cached.
        // Allow generous slack for slow CI: 10k iterations under 200 ms.
        Assert.True(sw.Elapsed < TimeSpan.FromMilliseconds(200),
            $"FieldCount/HasRows getters took {sw.Elapsed.TotalMilliseconds:F1}ms across {iterations} iterations — suspect missing init cache.");
    }

    [Fact]
    public async Task FirstRowBuffered_PriorPropertyAccess_DoesNotConsumeIt()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1, 2, 3";
        await using var reader = await cmd.ExecuteReaderAsync();

        // Property access pre-init — must NOT consume the first row.
        _ = reader.FieldCount;
        _ = reader.HasRows;

        Assert.True(reader.Read(), "First row should still be available after property access pre-Read.");
        Assert.Equal(1, reader.GetInt32(0));
        Assert.False(reader.Read(), "Only one row in result.");
    }
}

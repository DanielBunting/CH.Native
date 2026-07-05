using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Covers the friendly overlength message added to the FixedString direct-extraction path.
/// An over-long value yields a clear, column-scoped <see cref="ArgumentException"/> rather than
/// Encoding's opaque "destination too small". (Ported from the driver's FixedString tests.)
/// </summary>
[Collection("ClickHouse")]
public class FixedStringOverlengthTests
{
    private readonly ClickHouseFixture _fixture;

    public FixedStringOverlengthTests(ClickHouseFixture fixture) => _fixture = fixture;

    private sealed class FixedRow
    {
        [ClickHouseColumn(Name = "code")]
        public string Code { get; set; } = "";
    }

    [Fact]
    public async Task OverlongValue_ThrowsDescriptiveArgumentException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var table = $"fixed_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (code FixedString(4)) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<FixedRow>(table);
            await inserter.InitAsync();

            var ex = await Assert.ThrowsAnyAsync<ArgumentException>(async () =>
            {
                await inserter.AddAsync(new FixedRow { Code = "toolong" });
                await inserter.CompleteAsync();
            });
            Assert.Contains("FixedString(4)", ex.Message);
            Assert.Contains("code", ex.Message);
        }
        finally
        {
            // The mid-block-write failure poisons this connection (the INSERT handshake is left
            // half-done), so clean up on a fresh one.
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task ExactLengthValue_Succeeds()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        var table = $"fixed_{Guid.NewGuid():N}";
        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (code FixedString(4)) ENGINE = Memory");
        try
        {
            await using var inserter = connection.CreateBulkInserter<FixedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new FixedRow { Code = "abcd" });
            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {table}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

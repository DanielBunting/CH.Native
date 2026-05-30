using System.Collections;
using System.Data;
using System.Data.Common;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Drives the <see cref="DbDataReader"/> ADO surface that
/// <see cref="CH.Native.Results.ClickHouseDataReader"/> gained when it became a
/// <c>DbDataReader</c>. The reader is obtained through the
/// <see cref="DbCommand"/> path (as Dapper / ADO consumers do), which primes the
/// ADO first-row buffer — exercising the priming branches in <c>ReadCoreAsync</c>
/// and <c>EnsureInitializedForAdo</c> alongside the indexers, <c>NextResult</c>,
/// and <c>GetValues</c>.
/// </summary>
[Collection("ClickHouse")]
public class ClickHouseDataReaderAdoSurfaceTests
{
    private readonly ClickHouseFixture _fixture;

    public ClickHouseDataReaderAdoSurfaceTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<DbConnection> OpenAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    [Fact]
    public async Task DbCommandReader_ExposesMetadataAndRows()
    {
        await using var conn = await OpenAsync();
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt64(number) AS n, toString(number) AS s FROM numbers(3) ORDER BY number";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(2, reader.FieldCount);
        Assert.True(reader.HasRows);
        Assert.False(reader.IsClosed);
        Assert.Equal(0, reader.Depth);
        Assert.Equal(-1, reader.RecordsAffected);

        var rows = 0;
        while (await reader.ReadAsync())
        {
            // Ordinal + name indexers.
            var byOrdinal = Convert.ToInt64(reader[0]);
            var byName = Convert.ToInt64(reader["n"]);
            Assert.Equal(byOrdinal, byName);

            // GetValue + typed fetch.
            Assert.Equal(byOrdinal, Convert.ToInt64(reader.GetValue(0)));
            Assert.False(reader.IsDBNull(1));
            Assert.Equal(byOrdinal.ToString(), reader.GetFieldValue<string>(1));

            // GetValues fills the buffer.
            var buffer = new object[2];
            var copied = reader.GetValues(buffer);
            Assert.Equal(2, copied);

            rows++;
        }

        Assert.Equal(3, rows);

        // Single result set: NextResult is always false.
        Assert.False(reader.NextResult());
        Assert.False(await reader.NextResultAsync());
    }

    [Fact]
    public async Task DbCommandReader_SyncRead_Works()
    {
        await using var conn = await OpenAsync();
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt64(42) AS n";

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read()); // sync path
        Assert.Equal(42L, Convert.ToInt64(reader["n"]));
        Assert.False(reader.Read());
    }

    [Fact]
    public async Task DbCommandReader_EnumeratesAsDbDataRecords()
    {
        await using var conn = await OpenAsync();
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt64(number) AS n FROM numbers(4)";

        await using var reader = await cmd.ExecuteReaderAsync();

        // DbDataReader implements IEnumerable (yields DbDataRecord per row).
        var count = 0;
        foreach (var _ in (IEnumerable)reader)
            count++;

        Assert.Equal(4, count);
    }

    [Fact]
    public async Task DbCommandReader_EmptyResult_HasNoRows()
    {
        await using var conn = await OpenAsync();
        DbCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT toInt64(1) AS n WHERE 1 = 0";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(reader.HasRows);
        Assert.False(await reader.ReadAsync());
    }
}

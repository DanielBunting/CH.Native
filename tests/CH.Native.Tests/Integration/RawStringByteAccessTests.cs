using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

// ClickHouse String values are arbitrary byte sequences. Under
// StringMaterialization=Lazy, GetFieldValue<byte[]> recovers the exact stored bytes —
// including sequences that are not valid UTF-8, which the string accessors replace
// with U+FFFD. (Eager mode decodes during the block read, so byte recovery is a
// lazy-mode feature.)
[Collection("ClickHouse")]
public class RawStringByteAccessTests
{
    private readonly ClickHouseFixture _fixture;

    public RawStringByteAccessTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private string LazyConnectionString =>
        _fixture.ConnectionString + ";StringMaterialization=Lazy";

    private async Task RunWithTableAsync(string columnType, string insertSql, Func<ClickHouseConnection, string, Task> body)
    {
        var table = $"test_rawbytes_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync($"CREATE TABLE {table} (val {columnType}) ENGINE = Memory");
        try
        {
            await setup.ExecuteNonQueryAsync(string.Format(insertSql, table));

            await using var connection = new ClickHouseConnection(LazyConnectionString);
            await connection.OpenAsync();
            await body(connection, table);
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task InvalidUtf8_GetFieldValueBytes_RecoversExactBytes() =>
        RunWithTableAsync("String", "INSERT INTO {0} VALUES (unhex('FF61'))", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(new byte[] { 0xFF, 0x61 }, row.GetFieldValue<byte[]>(0));

                // The string view of the same value still applies U+FFFD replacement.
                Assert.Equal("�a", row.GetFieldValue<string>(0));
            }
        });

    [Fact]
    public Task ValidUtf8AndEmpty_GetFieldValueBytes_MatchesEncoding() =>
        RunWithTableAsync("String", "INSERT INTO {0} VALUES ('тест 🦀'),('')", async (connection, table) =>
        {
            var results = new List<byte[]>();
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table} ORDER BY val"))
            {
                results.Add(row.GetFieldValue<byte[]>(0));
            }

            Assert.Equal(2, results.Count);
            Assert.Empty(results[0]);
            Assert.Equal(System.Text.Encoding.UTF8.GetBytes("тест 🦀"), results[1]);
        });

    [Fact]
    public Task EmbeddedNul_GetFieldValueBytes_Preserved() =>
        RunWithTableAsync("String", "INSERT INTO {0} VALUES ('a\\0b')", async (connection, table) =>
        {
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table}"))
            {
                Assert.Equal(new byte[] { (byte)'a', 0x00, (byte)'b' }, row.GetFieldValue<byte[]>(0));
            }
        });

    [Fact]
    public Task NullableString_GetFieldValueBytes_NullRowGivesNull() =>
        RunWithTableAsync("Nullable(String)", "INSERT INTO {0} VALUES (unhex('FF61')),(NULL)", async (connection, table) =>
        {
            var results = new List<byte[]?>();
            await foreach (var row in connection.QueryStreamAsync($"SELECT val FROM {table} ORDER BY val NULLS LAST"))
            {
                results.Add(row.IsDBNull(0) ? null : row.GetFieldValue<byte[]>(0));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(new byte[] { 0xFF, 0x61 }, results[0]);
            Assert.Null(results[1]);
        });

    [Fact]
    public async Task AdoReader_GetFieldValueBytes_Works()
    {
        var table = $"test_rawbytes_{Guid.NewGuid():N}";
        await using var setup = new ClickHouseConnection(_fixture.ConnectionString);
        await setup.OpenAsync();
        await setup.ExecuteNonQueryAsync($"CREATE TABLE {table} (val String) ENGINE = Memory");
        try
        {
            await setup.ExecuteNonQueryAsync($"INSERT INTO {table} VALUES (unhex('FF61'))");

            await using var connection = new ClickHouseConnection(LazyConnectionString);
            await connection.OpenAsync();
            await using System.Data.Common.DbCommand cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT val FROM {table}";
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(new byte[] { 0xFF, 0x61 }, reader.GetFieldValue<byte[]>(0));
        }
        finally
        {
            await setup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

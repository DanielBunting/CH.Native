using CH.Native.BulkInsert;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Stress;

[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Stress)]
public class CompressionEdgeCaseTests
{
    private readonly SingleNodeFixture _fixture;

    public CompressionEdgeCaseTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Compression_LZ4_EmptyBlock()
    {
        var tableName = $"test_comp_lz4_empty_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(CompressionMethod.Lz4)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<CompressionRow>(tableName);
            await inserter.InitAsync();

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Compression_Zstd_EmptyBlock()
    {
        var tableName = $"test_comp_zstd_empty_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(CompressionMethod.Zstd)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<CompressionRow>(tableName);
            await inserter.InitAsync();

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(0, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Compression_LZ4_SingleByte()
    {
        var tableName = $"test_comp_lz4_byte_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(CompressionMethod.Lz4)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                value UInt8
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<SingleByteRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SingleByteRow { Value = 42 });
            await inserter.CompleteAsync();

            var result = await connection.ExecuteScalarAsync<byte>($"SELECT value FROM {tableName}");
            Assert.Equal((byte)42, result);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Compression_LZ4_LargeBlock_10MB()
    {
        var tableName = $"test_comp_lz4_large_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(CompressionMethod.Lz4)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                payload String
            ) ENGINE = Memory");

        try
        {
            var largeString = new string('A', 1024);
            var options = new BulkInsertOptions { BatchSize = 5000 };
            await using var inserter = connection.CreateBulkInserter<PayloadRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 10_000; i++)
            {
                await inserter.AddAsync(new PayloadRow { Id = i, Payload = largeString });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10_000, count);

            // Verify payload integrity at multiple sample points (start, middle, end)
            // to catch corruption that a single-row sample would miss.
            foreach (var id in new[] { 0, 5000, 9999 })
            {
                var sample = await connection.ExecuteScalarAsync<string>(
                    $"SELECT payload FROM {tableName} WHERE id = {id}");
                Assert.Equal(largeString, sample);
            }
            // Strongest check: any divergent payload anywhere in the table.
            var divergent = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE payload != '{new string('A', 1024)}'");
            Assert.Equal(0, divergent);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Compression_Zstd_LargeBlock_10MB()
    {
        var tableName = $"test_comp_zstd_large_{Guid.NewGuid():N}";
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithCredentials(_fixture.Username, _fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(CompressionMethod.Zstd)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                payload String
            ) ENGINE = Memory");

        try
        {
            var largeString = new string('B', 1024);
            var options = new BulkInsertOptions { BatchSize = 5000 };
            await using var inserter = connection.CreateBulkInserter<PayloadRow>(tableName, options);
            await inserter.InitAsync();

            for (int i = 0; i < 10_000; i++)
            {
                await inserter.AddAsync(new PayloadRow { Id = i, Payload = largeString });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(10_000, count);

            foreach (var id in new[] { 0, 5000, 9999 })
            {
                var sample = await connection.ExecuteScalarAsync<string>(
                    $"SELECT payload FROM {tableName} WHERE id = {id}");
                Assert.Equal(largeString, sample);
            }
            var divergent = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName} WHERE payload != '{new string('B', 1024)}'");
            Assert.Equal(0, divergent);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Compression_LZ4_Insert_Zstd_Query()
    {
        var tableName = $"test_comp_cross_{Guid.NewGuid():N}";

        await using var setupConn = new ClickHouseConnection(_fixture.ConnectionString);
        await setupConn.OpenAsync();

        await setupConn.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory");

        try
        {
            var lz4Settings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(_fixture.Username, _fixture.Password)
                .WithCompression(true)
                .WithCompressionMethod(CompressionMethod.Lz4)
                .Build();

            await using var lz4Connection = new ClickHouseConnection(lz4Settings);
            await lz4Connection.OpenAsync();

            await using var inserter = lz4Connection.CreateBulkInserter<CompressionRow>(tableName);
            await inserter.InitAsync();

            for (int i = 0; i < 1000; i++)
            {
                await inserter.AddAsync(new CompressionRow { Id = i, Name = $"cross_comp_{i}" });
            }

            await inserter.CompleteAsync();

            var zstdSettings = ClickHouseConnectionSettings.CreateBuilder()
                .WithHost(_fixture.Host)
                .WithPort(_fixture.Port)
                .WithCredentials(_fixture.Username, _fixture.Password)
                .WithCompression(true)
                .WithCompressionMethod(CompressionMethod.Zstd)
                .Build();

            await using var zstdConnection = new ClickHouseConnection(zstdSettings);
            await zstdConnection.OpenAsync();

            var count = await zstdConnection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1000, count);

            var sampleName = await zstdConnection.ExecuteScalarAsync<string>(
                $"SELECT name FROM {tableName} WHERE id = 500");
            Assert.Equal("cross_comp_500", sampleName);
        }
        finally
        {
            await setupConn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    private class CompressionRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "name", Order = 1)]
        public string Name { get; set; } = string.Empty;
    }

    private class SingleByteRow
    {
        [ClickHouseColumn(Name = "value", Order = 0)]
        public byte Value { get; set; }
    }

    private class PayloadRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "payload", Order = 1)]
        public string Payload { get; set; } = string.Empty;
    }
}

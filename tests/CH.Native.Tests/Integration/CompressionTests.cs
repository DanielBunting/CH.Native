using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class CompressionTests
{
    private readonly ClickHouseFixture _fixture;

    public CompressionTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_ReturnsCorrectResults(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>("SELECT 42");

        Assert.Equal(42, result);
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_SelectString(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<string>("SELECT 'Hello, World!'");

        Assert.Equal("Hello, World!", result);
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_SelectMultipleRows(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT number FROM system.numbers LIMIT 10");

        var count = 0;
        while (await reader.ReadAsync())
        {
            var value = reader.GetFieldValue<ulong>(0);
            Assert.Equal((ulong)count, value);
            count++;
        }

        Assert.Equal(10, count);
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_LargeResultSet(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT number, toString(number) FROM system.numbers LIMIT 10000");

        var count = 0;
        while (await reader.ReadAsync())
        {
            var num = reader.GetFieldValue<ulong>(0);
            var str = reader.GetFieldValue<string>(1);
            Assert.Equal((ulong)count, num);
            Assert.Equal(count.ToString(), str);
            count++;
        }

        Assert.Equal(10000, count);
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_MatchesUncompressed(CompressionMethod method)
    {
        // First, query without compression
        await using var uncompressedConnection = new ClickHouseConnection(_fixture.ConnectionString);
        await uncompressedConnection.OpenAsync();

        var uncompressedResults = new List<(ulong, string)>();
        await using (var reader = await uncompressedConnection.ExecuteReaderAsync("SELECT number, toString(number) FROM system.numbers LIMIT 100"))
        {
            while (await reader.ReadAsync())
            {
                uncompressedResults.Add((reader.GetFieldValue<ulong>(0), reader.GetFieldValue<string>(1)));
            }
        }

        // Then, query with compression
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var compressedConnection = new ClickHouseConnection(settings);
        await compressedConnection.OpenAsync();

        var compressedResults = new List<(ulong, string)>();
        await using (var reader = await compressedConnection.ExecuteReaderAsync("SELECT number, toString(number) FROM system.numbers LIMIT 100"))
        {
            while (await reader.ReadAsync())
            {
                compressedResults.Add((reader.GetFieldValue<ulong>(0), reader.GetFieldValue<string>(1)));
            }
        }

        // Results should match
        Assert.Equal(uncompressedResults.Count, compressedResults.Count);
        for (int i = 0; i < uncompressedResults.Count; i++)
        {
            Assert.Equal(uncompressedResults[i].Item1, compressedResults[i].Item1);
            Assert.Equal(uncompressedResults[i].Item2, compressedResults[i].Item2);
        }
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_AllBasicTypes(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(@"
            SELECT
                toInt8(42) as i8,
                toInt16(1000) as i16,
                toInt32(100000) as i32,
                toInt64(10000000000) as i64,
                toUInt8(200) as u8,
                toUInt16(60000) as u16,
                toUInt32(4000000000) as u32,
                toUInt64(10000000000000000000) as u64,
                toFloat32(3.14) as f32,
                toFloat64(3.14159265358979) as f64,
                'test string' as str
        ");

        Assert.True(await reader.ReadAsync());

        Assert.Equal((sbyte)42, reader.GetFieldValue<sbyte>(0));
        Assert.Equal((short)1000, reader.GetFieldValue<short>(1));
        Assert.Equal(100000, reader.GetFieldValue<int>(2));
        Assert.Equal(10000000000L, reader.GetFieldValue<long>(3));
        Assert.Equal((byte)200, reader.GetFieldValue<byte>(4));
        Assert.Equal((ushort)60000, reader.GetFieldValue<ushort>(5));
        Assert.Equal(4000000000U, reader.GetFieldValue<uint>(6));
        Assert.Equal(10000000000000000000UL, reader.GetFieldValue<ulong>(7));
        Assert.Equal(3.14f, reader.GetFieldValue<float>(8), 2);
        Assert.Equal(3.14159265358979, reader.GetFieldValue<double>(9), 10);
        Assert.Equal("test string", reader.GetFieldValue<string>(10));
    }

    [Fact]
    public async Task ConnectionString_WithCompression_ParsesCorrectly()
    {
        var connectionString = $"Host={_fixture.Host};Port={_fixture.Port};Username={_fixture.Username};Password={_fixture.Password};Compress=true;CompressionMethod=lz4";
        var settings = ClickHouseConnectionSettings.Parse(connectionString);

        Assert.True(settings.Compress);
        Assert.Equal(CompressionMethod.Lz4, settings.CompressionMethod);
    }

    [Fact]
    public async Task ConnectionString_WithZstdCompression_ParsesCorrectly()
    {
        var connectionString = $"Host={_fixture.Host};Port={_fixture.Port};Username={_fixture.Username};Password={_fixture.Password};Compress=true;CompressionMethod=zstd";
        var settings = ClickHouseConnectionSettings.Parse(connectionString);

        Assert.True(settings.Compress);
        Assert.Equal(CompressionMethod.Zstd, settings.CompressionMethod);
    }

    [Fact]
    public async Task Query_WithCompressionFromConnectionString_Works()
    {
        var connectionString = $"Host={_fixture.Host};Port={_fixture.Port};Username={_fixture.Username};Password={_fixture.Password};Compress=true;CompressionMethod=lz4";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        var result = await connection.ExecuteScalarAsync<int>("SELECT 123");

        Assert.Equal(123, result);
    }

    [Theory]
    [InlineData(CompressionMethod.Lz4)]
    [InlineData(CompressionMethod.Zstd)]
    public async Task Query_WithCompression_MultipleQueries(CompressionMethod method)
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost(_fixture.Host)
            .WithPort(_fixture.Port)
            .WithUsername(_fixture.Username)
            .WithPassword(_fixture.Password)
            .WithCompression(true)
            .WithCompressionMethod(method)
            .Build();

        await using var connection = new ClickHouseConnection(settings);
        await connection.OpenAsync();

        // Execute multiple queries on the same connection
        for (int i = 0; i < 5; i++)
        {
            var result = await connection.ExecuteScalarAsync<int>($"SELECT {i}");
            Assert.Equal(i, result);
        }
    }
}

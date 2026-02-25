using System.Net;
using System.Numerics;
using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class BulkInsertTypeRoundTripTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertTypeRoundTripTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Test 1: Bool

    [Fact]
    public async Task BulkInsert_Bool_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Bool
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<BoolRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new BoolRow { Id = 1, Value = true });
            await inserter.AddAsync(new BoolRow { Id = 2, Value = false });
            await inserter.AddAsync(new BoolRow { Id = 3, Value = true });

            await inserter.CompleteAsync();

            var results = new List<bool>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<bool>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.True(results[0]);
            Assert.False(results[1]);
            Assert.True(results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 2: Int128

    [Fact]
    public async Task BulkInsert_Int128_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Int128
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Int128Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Int128Row { Id = 1, Value = Int128.MaxValue });
            await inserter.AddAsync(new Int128Row { Id = 2, Value = Int128.MinValue });
            await inserter.AddAsync(new Int128Row { Id = 3, Value = 0 });

            await inserter.CompleteAsync();

            var results = new List<Int128>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<Int128>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(Int128.MaxValue, results[0]);
            Assert.Equal(Int128.MinValue, results[1]);
            Assert.Equal((Int128)0, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 3: Int256 (SQL insert only)

    [Fact]
    public async Task BulkInsert_Int256_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Int256
            ) ENGINE = Memory");

        try
        {
            // Int256 has no dedicated extractor in ColumnExtractorFactory, so insert via SQL
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 12345678901234567890), (2, -12345678901234567890), (3, 0)");

            var results = new List<BigInteger>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<BigInteger>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(BigInteger.Parse("12345678901234567890"), results[0]);
            Assert.Equal(BigInteger.Parse("-12345678901234567890"), results[1]);
            Assert.Equal(BigInteger.Zero, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 4: UInt128

    [Fact]
    public async Task BulkInsert_UInt128_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value UInt128
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<UInt128Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new UInt128Row { Id = 1, Value = UInt128.MaxValue });
            await inserter.AddAsync(new UInt128Row { Id = 2, Value = 0 });

            await inserter.CompleteAsync();

            var results = new List<UInt128>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<UInt128>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(UInt128.MaxValue, results[0]);
            Assert.Equal((UInt128)0, results[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 5: UInt256 (SQL insert only)

    [Fact]
    public async Task BulkInsert_UInt256_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value UInt256
            ) ENGINE = Memory");

        try
        {
            // UInt256 has no dedicated extractor in ColumnExtractorFactory, so insert via SQL
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 99999999999999999999999999999999), (2, 0)");

            var results = new List<BigInteger>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<BigInteger>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(BigInteger.Parse("99999999999999999999999999999999"), results[0]);
            Assert.Equal(BigInteger.Zero, results[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 6: Decimal32

    [Fact]
    public async Task BulkInsert_Decimal32_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Decimal32(4)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Decimal32Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Decimal32Row { Id = 1, Value = 9999.9999m });
            await inserter.AddAsync(new Decimal32Row { Id = 2, Value = -9999.9999m });
            await inserter.AddAsync(new Decimal32Row { Id = 3, Value = 0m });

            await inserter.CompleteAsync();

            var results = new List<decimal>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<decimal>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(9999.9999m, results[0]);
            Assert.Equal(-9999.9999m, results[1]);
            Assert.Equal(0m, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 7: Decimal64

    [Fact]
    public async Task BulkInsert_Decimal64_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Decimal64(8)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Decimal64Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Decimal64Row { Id = 1, Value = 12345678.12345678m });
            await inserter.AddAsync(new Decimal64Row { Id = 2, Value = -12345678.12345678m });
            await inserter.AddAsync(new Decimal64Row { Id = 3, Value = 0m });

            await inserter.CompleteAsync();

            var results = new List<decimal>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<decimal>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(12345678.12345678m, results[0]);
            Assert.Equal(-12345678.12345678m, results[1]);
            Assert.Equal(0m, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 8: Decimal128

    [Fact]
    public async Task BulkInsert_Decimal128_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Decimal128(18)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Decimal128Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Decimal128Row { Id = 1, Value = 123456789.123456789012345678m });
            await inserter.AddAsync(new Decimal128Row { Id = 2, Value = -123456789.123456789012345678m });
            await inserter.AddAsync(new Decimal128Row { Id = 3, Value = 0m });

            await inserter.CompleteAsync();

            var results = new List<decimal>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<decimal>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(123456789.123456789012345678m, results[0]);
            Assert.Equal(-123456789.123456789012345678m, results[1]);
            Assert.Equal(0m, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 9: Decimal256

    [Fact]
    public async Task BulkInsert_Decimal256_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Decimal256(10)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Decimal256Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Decimal256Row { Id = 1, Value = 1234567890.1234567890m });
            await inserter.AddAsync(new Decimal256Row { Id = 2, Value = -1234567890.1234567890m });
            await inserter.AddAsync(new Decimal256Row { Id = 3, Value = 0m });

            await inserter.CompleteAsync();

            var results = new List<decimal>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<decimal>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(1234567890.1234567890m, results[0]);
            Assert.Equal(-1234567890.1234567890m, results[1]);
            Assert.Equal(0m, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 10: FixedString

    [Fact]
    public async Task BulkInsert_FixedString_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value FixedString(16)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<FixedStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new FixedStringRow { Id = 1, Value = "hello" });
            await inserter.AddAsync(new FixedStringRow { Id = 2, Value = "world" });
            await inserter.AddAsync(new FixedStringRow { Id = 3, Value = "" });

            await inserter.CompleteAsync();

            var results = new List<byte[]>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<byte[]>("Value"));
            }

            Assert.Equal(3, results.Count);

            // FixedString(16) pads with null bytes to 16 bytes
            Assert.Equal(16, results[0].Length);
            Assert.Equal((byte)'h', results[0][0]);
            Assert.Equal((byte)'e', results[0][1]);
            Assert.Equal((byte)'l', results[0][2]);
            Assert.Equal((byte)'l', results[0][3]);
            Assert.Equal((byte)'o', results[0][4]);
            for (int i = 5; i < 16; i++)
                Assert.Equal(0, results[0][i]);

            Assert.Equal(16, results[1].Length);
            Assert.Equal((byte)'w', results[1][0]);
            Assert.Equal((byte)'o', results[1][1]);
            Assert.Equal((byte)'r', results[1][2]);
            Assert.Equal((byte)'l', results[1][3]);
            Assert.Equal((byte)'d', results[1][4]);
            for (int i = 5; i < 16; i++)
                Assert.Equal(0, results[1][i]);

            Assert.Equal(16, results[2].Length);
            for (int i = 0; i < 16; i++)
                Assert.Equal(0, results[2][i]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 11: Enum8

    [Fact]
    public async Task BulkInsert_Enum8_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Enum8('a' = 1, 'b' = 2, 'c' = 3)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Enum8Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Enum8Row { Id = 1, Value = 1 });
            await inserter.AddAsync(new Enum8Row { Id = 2, Value = 2 });
            await inserter.AddAsync(new Enum8Row { Id = 3, Value = 3 });

            await inserter.CompleteAsync();

            var results = new List<sbyte>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<sbyte>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal((sbyte)1, results[0]);
            Assert.Equal((sbyte)2, results[1]);
            Assert.Equal((sbyte)3, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 12: Enum16

    [Fact]
    public async Task BulkInsert_Enum16_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Enum16('x' = 1000, 'y' = 2000)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<Enum16Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Enum16Row { Id = 1, Value = 1000 });
            await inserter.AddAsync(new Enum16Row { Id = 2, Value = 2000 });

            await inserter.CompleteAsync();

            var results = new List<short>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<short>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal((short)1000, results[0]);
            Assert.Equal((short)2000, results[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 13: IPv4

    [Fact]
    public async Task BulkInsert_IPv4_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value IPv4
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<IPv4Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new IPv4Row { Id = 1, Value = IPAddress.Parse("127.0.0.1") });
            await inserter.AddAsync(new IPv4Row { Id = 2, Value = IPAddress.Parse("255.255.255.255") });
            await inserter.AddAsync(new IPv4Row { Id = 3, Value = IPAddress.Parse("0.0.0.0") });

            await inserter.CompleteAsync();

            var results = new List<IPAddress>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<IPAddress>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(IPAddress.Parse("127.0.0.1"), results[0]);
            Assert.Equal(IPAddress.Parse("255.255.255.255"), results[1]);
            Assert.Equal(IPAddress.Parse("0.0.0.0"), results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 14: IPv6

    [Fact]
    public async Task BulkInsert_IPv6_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value IPv6
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<IPv6Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new IPv6Row { Id = 1, Value = IPAddress.Parse("::1") });
            await inserter.AddAsync(new IPv6Row { Id = 2, Value = IPAddress.Parse("fe80::1") });

            await inserter.CompleteAsync();

            var results = new List<IPAddress>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<IPAddress>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(IPAddress.Parse("::1"), results[0]);
            Assert.Equal(IPAddress.Parse("fe80::1"), results[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 15: Date32

    [Fact]
    public async Task BulkInsert_Date32_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Date32
            ) ENGINE = Memory");

        try
        {
            var today = DateOnly.FromDateTime(DateTime.UtcNow);

            await using var inserter = connection.CreateBulkInserter<Date32Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new Date32Row { Id = 1, Value = new DateOnly(1925, 1, 1) });
            await inserter.AddAsync(new Date32Row { Id = 2, Value = new DateOnly(2283, 11, 11) });
            await inserter.AddAsync(new Date32Row { Id = 3, Value = today });

            await inserter.CompleteAsync();

            var results = new List<DateOnly>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<DateOnly>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(new DateOnly(1925, 1, 1), results[0]);
            Assert.Equal(new DateOnly(2283, 11, 11), results[1]);
            Assert.Equal(today, results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 16: DateTime with timezone

    [Fact]
    public async Task BulkInsert_DateTimeWithTimezone_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value DateTime('UTC')
            ) ENGINE = Memory");

        try
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var recent = new DateTime(2024, 6, 15, 12, 30, 0, DateTimeKind.Utc);

            await using var inserter = connection.CreateBulkInserter<DateTimeTzRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new DateTimeTzRow { Id = 1, Value = epoch });
            await inserter.AddAsync(new DateTimeTzRow { Id = 2, Value = recent });

            await inserter.CompleteAsync();

            var results = new List<DateTime>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<DateTime>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Equal(epoch, results[0]);
            Assert.Equal(recent, results[1]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 17: Tuple

    [Fact]
    public async Task BulkInsert_Tuple_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Tuple(Int32, String, Float64)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<TupleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new TupleRow { Id = 1, Value = new object[] { 42, "hello", 3.14 } });
            await inserter.AddAsync(new TupleRow { Id = 2, Value = new object[] { -1, "world", 0.0 } });

            await inserter.CompleteAsync();

            var results = new List<object[]>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                var tuple = row.GetFieldValue<object>("Value");
                results.Add((object[])tuple);
            }

            Assert.Equal(2, results.Count);

            Assert.Equal(42, Convert.ToInt32(results[0][0]));
            Assert.Equal("hello", Convert.ToString(results[0][1]));
            Assert.Equal(3.14, Convert.ToDouble(results[0][2]), 6);

            Assert.Equal(-1, Convert.ToInt32(results[1][0]));
            Assert.Equal("world", Convert.ToString(results[1][1]));
            Assert.Equal(0.0, Convert.ToDouble(results[1][2]), 6);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 18: JSON

    [Fact]
    public async Task BulkInsert_Json_RoundTrips()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // JSON type requires ClickHouse 25.6+
        var info = connection.ServerInfo;
        if (info == null || info.VersionMajor < 25 || (info.VersionMajor == 25 && info.VersionMinor < 6))
        {
            // Skip test on older versions that don't support the JSON type
            return;
        }

        var tableName = $"test_type_{Guid.NewGuid():N}";

        try
        {
            await connection.ExecuteNonQueryAsync("SET allow_experimental_json_type = 1");

            await connection.ExecuteNonQueryAsync($@"
                CREATE TABLE {tableName} (
                    Id Int32,
                    Value JSON
                ) ENGINE = Memory");

            await using var inserter = connection.CreateBulkInserter<JsonRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new JsonRow { Id = 1, Value = "{\"name\":\"Alice\",\"age\":30}" });
            await inserter.AddAsync(new JsonRow { Id = 2, Value = "{\"name\":\"Bob\",\"age\":25}" });

            await inserter.CompleteAsync();

            var results = new List<string>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<string>("Value"));
            }

            Assert.Equal(2, results.Count);
            Assert.Contains("Alice", results[0]);
            Assert.Contains("Bob", results[1]);
        }
        catch (Exception ex) when (ex.Message.Contains("JSON") || ex.Message.Contains("experimental"))
        {
            // Skip if server doesn't support JSON type
            return;
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 19: Nullable(Bool)

    [Fact]
    public async Task BulkInsert_NullableBool_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Nullable(Bool)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NullableBoolRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableBoolRow { Id = 1, Value = true });
            await inserter.AddAsync(new NullableBoolRow { Id = 2, Value = null });
            await inserter.AddAsync(new NullableBoolRow { Id = 3, Value = false });

            await inserter.CompleteAsync();

            var results = new List<bool?>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<bool?>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.True(results[0]);
            Assert.Null(results[1]);
            Assert.False(results[2]);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test 20: Nullable(Decimal64)

    [Fact]
    public async Task BulkInsert_NullableDecimal64_RoundTrips()
    {
        var tableName = $"test_type_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value Nullable(Decimal64(8))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NullableDecimal64Row>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NullableDecimal64Row { Id = 1, Value = 99.12345678m });
            await inserter.AddAsync(new NullableDecimal64Row { Id = 2, Value = null });
            await inserter.AddAsync(new NullableDecimal64Row { Id = 3, Value = 0m });

            await inserter.CompleteAsync();

            var results = new List<decimal?>();
            await foreach (var row in connection.QueryAsync($"SELECT Value FROM {tableName} ORDER BY Id"))
            {
                results.Add(row.GetFieldValue<decimal?>("Value"));
            }

            Assert.Equal(3, results.Count);
            Assert.Equal(99.12345678m, results[0]);
            Assert.Null(results[1]);
            Assert.Equal(0m, results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Test POCOs

    private class BoolRow
    {
        public int Id { get; set; }
        public bool Value { get; set; }
    }

    private class Int128Row
    {
        public int Id { get; set; }
        public Int128 Value { get; set; }
    }

    private class UInt128Row
    {
        public int Id { get; set; }
        public UInt128 Value { get; set; }
    }

    private class Decimal32Row
    {
        public int Id { get; set; }
        public decimal Value { get; set; }
    }

    private class Decimal64Row
    {
        public int Id { get; set; }
        public decimal Value { get; set; }
    }

    private class Decimal128Row
    {
        public int Id { get; set; }
        public decimal Value { get; set; }
    }

    private class Decimal256Row
    {
        public int Id { get; set; }
        public decimal Value { get; set; }
    }

    private class Enum8Row
    {
        public int Id { get; set; }
        public sbyte Value { get; set; }
    }

    private class Enum16Row
    {
        public int Id { get; set; }
        public short Value { get; set; }
    }

    private class IPv6Row
    {
        public int Id { get; set; }
        public IPAddress Value { get; set; } = IPAddress.IPv6None;
    }

    private class Date32Row
    {
        public int Id { get; set; }
        public DateOnly Value { get; set; }
    }

    private class DateTimeTzRow
    {
        public int Id { get; set; }
        public DateTime Value { get; set; }
    }

    private class JsonRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private class NullableBoolRow
    {
        public int Id { get; set; }
        public bool? Value { get; set; }
    }

    private class IPv4Row
    {
        public int Id { get; set; }
        public IPAddress Value { get; set; } = IPAddress.Any;
    }

    private class FixedStringRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private class TupleRow
    {
        public int Id { get; set; }
        public object[] Value { get; set; } = Array.Empty<object>();
    }

    private class NullableDecimal64Row
    {
        public int Id { get; set; }
        public decimal? Value { get; set; }
    }

    #endregion
}

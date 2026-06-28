using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Cluster: structural variations of Nested columns — multiple Nested columns in one
/// table, large row counts (offset accumulation), and many-field Nested. All prior
/// Nested tests used a single column and 2–3 rows.
/// </summary>
[Collection("ClickHouse")]
public class NestedStructuralIntegrationTests
{
    private readonly ClickHouseFixture _fixture;

    public NestedStructuralIntegrationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<ClickHouseConnection> OpenUnflattenedAsync(bool compress = true)
    {
        var cs = compress ? _fixture.ConnectionString : _fixture.ConnectionString + ";Compress=false";
        var conn = new ClickHouseConnection(cs);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync("SET flatten_nested = 0");
        return conn;
    }

    private class TwoNestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "a", Order = 1)] public object[] A { get; set; } = Array.Empty<object>();
        [ClickHouseColumn(Name = "b", Order = 2)] public object[] B { get; set; } = Array.Empty<object>();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)] // uncompressed → also exercises the skipper over two Nested columns
    public async Task MultipleNestedColumns_RoundTrip(bool compress)
    {
        var table = $"nstruct_{Guid.NewGuid():N}";
        await using var conn = await OpenUnflattenedAsync(compress);
        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {table} (
                id Int32,
                a Nested(k String, v Int32),
                b Nested(x Float64)
            ) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<TwoNestedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new TwoNestedRow
            {
                Id = 1,
                A = new object[] { new[] { "p", "q" }, new[] { 1, 2 } },
                B = new object[] { new[] { 1.5, 2.5, 3.5 } },
            });
            await inserter.AddAsync(new TwoNestedRow
            {
                Id = 2,
                A = new object[] { Array.Empty<string>(), Array.Empty<int>() },
                B = new object[] { new[] { 9.9 } },
            });
            await inserter.CompleteAsync();

            var rows = new List<(string[] k, int[] v, double[] x)>();
            await foreach (var row in conn.QueryStreamAsync($"SELECT a, b FROM {table} ORDER BY id"))
            {
                var a = (object[])row.GetFieldValue<object>("a");
                var b = (object[])row.GetFieldValue<object>("b");
                rows.Add(((string[])a[0], (int[])a[1], (double[])b[0]));
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { "p", "q" }, rows[0].k);
            Assert.Equal(new[] { 1, 2 }, rows[0].v);
            Assert.Equal(new[] { 1.5, 2.5, 3.5 }, rows[0].x);
            Assert.Empty(rows[1].k);
            Assert.Equal(new[] { 9.9 }, rows[1].x);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class OneNestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "n", Order = 1)] public object[] N { get; set; } = Array.Empty<object>();
    }

    [Fact]
    public async Task LargeRowCount_VaryingLengths_RoundTrips()
    {
        const int rowCount = 1000;
        var table = $"nstruct_big_{Guid.NewGuid():N}";
        await using var conn = await OpenUnflattenedAsync();
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, n Nested(key String, value Int64)) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<OneNestedRow>(table);
            await inserter.InitAsync();

            long expectedTotalElements = 0;
            long expectedValueSum = 0;
            for (int i = 0; i < rowCount; i++)
            {
                int len = i % 7; // 0..6 elements, many empties
                var keys = new string[len];
                var vals = new long[len];
                for (int j = 0; j < len; j++)
                {
                    keys[j] = $"k{i}_{j}";
                    vals[j] = (long)i * 1000 + j;
                    expectedValueSum += vals[j];
                }
                expectedTotalElements += len;
                await inserter.AddAsync(new OneNestedRow { Id = i, N = new object[] { keys, vals } });
            }
            await inserter.CompleteAsync();

            // Server-side aggregates over the flattened arrays confirm every element landed.
            Assert.Equal(rowCount, await conn.ExecuteScalarAsync<long>($"SELECT count() FROM {table}"));
            Assert.Equal(expectedTotalElements, await conn.ExecuteScalarAsync<long>($"SELECT sum(length(n.key)) FROM {table}"));
            Assert.Equal(expectedValueSum, await conn.ExecuteScalarAsync<long>($"SELECT sum(arraySum(n.value)) FROM {table}"));

            // Spot-check a non-trivial row via whole-column read.
            long checkedRows = 0;
            await foreach (var row in conn.QueryStreamAsync($"SELECT n FROM {table} WHERE id = 6"))
            {
                var n = (object[])row.GetFieldValue<object>("n");
                Assert.Equal(new[] { "k6_0", "k6_1", "k6_2", "k6_3", "k6_4", "k6_5" }, (string[])n[0]);
                Assert.Equal(new[] { 6000L, 6001L, 6002L, 6003L, 6004L, 6005L }, (long[])n[1]);
                checkedRows++;
            }
            Assert.Equal(1, checkedRows);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task ManyFieldNested_RoundTrips()
    {
        var table = $"nstruct_many_{Guid.NewGuid():N}";
        await using var conn = await OpenUnflattenedAsync();
        await conn.ExecuteNonQueryAsync($@"
            CREATE TABLE {table} (
                id Int32,
                n Nested(f1 Int32, f2 String, f3 Float64, f4 Int64, f5 String, f6 UInt8)
            ) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<OneNestedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new OneNestedRow
            {
                Id = 1,
                N = new object[]
                {
                    new[] { 1, 2 },
                    new[] { "a", "b" },
                    new[] { 1.1, 2.2 },
                    new[] { 100L, 200L },
                    new[] { "x", "y" },
                    new byte[] { 7, 8 },
                },
            });
            await inserter.CompleteAsync();

            await foreach (var row in conn.QueryStreamAsync($"SELECT n FROM {table}"))
            {
                var n = (object[])row.GetFieldValue<object>("n");
                Assert.Equal(6, n.Length);
                Assert.Equal(new[] { 1, 2 }, (int[])n[0]);
                Assert.Equal(new[] { "a", "b" }, (string[])n[1]);
                Assert.Equal(new[] { 1.1, 2.2 }, (double[])n[2]);
                Assert.Equal(new[] { 100L, 200L }, (long[])n[3]);
                Assert.Equal(new[] { "x", "y" }, (string[])n[4]);
                Assert.Equal(new byte[] { 7, 8 }, (byte[])n[5]);
            }
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

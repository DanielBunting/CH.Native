using CH.Native.BulkInsert;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class BulkInsertCompositeTypeTests
{
    private readonly ClickHouseFixture _fixture;

    public BulkInsertCompositeTypeTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Array of Array

    [Fact]
    public async Task BulkInsert_ArrayOfArray_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                nested_arr Array(Array(Int32))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<ArrayOfArrayRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new ArrayOfArrayRow { Id = 1, NestedArr = new[] { new[] { 1, 2 }, new[] { 3 } } });
            await inserter.AddAsync(new ArrayOfArrayRow { Id = 2, NestedArr = new[] { Array.Empty<int>() } });
            await inserter.AddAsync(new ArrayOfArrayRow { Id = 3, NestedArr = Array.Empty<int[]>() });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            var results = new List<(int id, int[][] arr)>();
            await foreach (var row in connection.QueryAsync($"SELECT id, nested_arr FROM {tableName} ORDER BY id"))
            {
                var id = row.GetFieldValue<int>("id");
                var arr = (int[][])row.GetFieldValue<object>("nested_arr");
                results.Add((id, arr));
            }

            Assert.Equal(3, results.Count);

            // Row 1: [[1,2],[3]]
            Assert.Equal(2, results[0].arr.Length);
            Assert.Equal(new[] { 1, 2 }, results[0].arr[0]);
            Assert.Equal(new[] { 3 }, results[0].arr[1]);

            // Row 2: [[]]
            Assert.Single(results[1].arr);
            Assert.Empty(results[1].arr[0]);

            // Row 3: []
            Assert.Empty(results[2].arr);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Array of Nullable

    [Fact]
    public async Task BulkInsert_ArrayOfNullable_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                tags Array(Nullable(String))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<ArrayOfNullableRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new ArrayOfNullableRow { Id = 1, Tags = new[] { "a", null, "b" } });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            await foreach (var row in connection.QueryAsync($"SELECT tags FROM {tableName}"))
            {
                var tags = (string?[])row.GetFieldValue<object>("tags");
                Assert.Equal(3, tags.Length);
                Assert.Equal("a", tags[0]);
                Assert.Null(tags[1]);
                Assert.Equal("b", tags[2]);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Map with Nullable Value

    [Fact]
    public async Task BulkInsert_MapWithNullableValue_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                metadata Map(String, Nullable(Int32))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<MapWithNullableValueRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MapWithNullableValueRow
            {
                Id = 1,
                Metadata = new Dictionary<string, int?> { ["a"] = 10, ["b"] = null, ["c"] = 30 }
            });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            await foreach (var row in connection.QueryAsync($"SELECT metadata FROM {tableName}"))
            {
                var map = (Dictionary<string, int?>)row.GetFieldValue<object>("metadata");
                Assert.Equal(3, map.Count);
                Assert.Equal(10, map["a"]);
                Assert.Null(map["b"]);
                Assert.Equal(30, map["c"]);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Map of Array

    [Fact]
    public async Task BulkInsert_MapOfArray_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                data Map(String, Array(Int32))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<MapOfArrayRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new MapOfArrayRow
            {
                Id = 1,
                Data = new Dictionary<string, int[]> { ["key"] = new[] { 1, 2, 3 } }
            });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            await foreach (var row in connection.QueryAsync($"SELECT data FROM {tableName}"))
            {
                var map = (Dictionary<string, int[]>)row.GetFieldValue<object>("data");
                Assert.Single(map);
                Assert.Equal(new[] { 1, 2, 3 }, map["key"]);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Tuple with Nullable

    [Fact]
    public async Task BulkInsert_TupleWithNullable_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                pair Tuple(Nullable(String), Int32)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<TupleWithNullableRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new TupleWithNullableRow { Id = 1, Pair = new object?[] { null, 42 } });
            await inserter.AddAsync(new TupleWithNullableRow { Id = 2, Pair = new object?[] { "hello", 99 } });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(2, count);

            var results = new List<System.Runtime.CompilerServices.ITuple>();
            await foreach (var row in connection.QueryAsync($"SELECT pair FROM {tableName} ORDER BY id"))
            {
                results.Add((System.Runtime.CompilerServices.ITuple)row.GetFieldValue<object>("pair"));
            }

            Assert.Equal(2, results.Count);
            Assert.Null(results[0][0]);
            Assert.Equal(42, results[0][1]);
            Assert.Equal("hello", results[1][0]);
            Assert.Equal(99, results[1][1]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Array of Tuple

    [Fact]
    public async Task BulkInsert_ArrayOfTuple_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                items Array(Tuple(Int32, String))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<ArrayOfTupleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new ArrayOfTupleRow
            {
                Id = 1,
                Items = new[] { new object[] { 1, "one" }, new object[] { 2, "two" } }
            });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);

            await foreach (var row in connection.QueryAsync($"SELECT items FROM {tableName}"))
            {
                var items = (object[])row.GetFieldValue<object>("items");
                Assert.Equal(2, items.Length);
                var item0 = (System.Runtime.CompilerServices.ITuple)items[0];
                var item1 = (System.Runtime.CompilerServices.ITuple)items[1];
                Assert.Equal(1, item0[0]);
                Assert.Equal("one", item0[1]);
                Assert.Equal(2, item1[0]);
                Assert.Equal("two", item1[1]);
            }
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region LowCardinality FixedString

    [Fact]
    public async Task BulkInsert_LowCardinalityFixedString_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                code LowCardinality(FixedString(8))
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<LowCardinalityFixedStringRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new LowCardinalityFixedStringRow { Id = 1, Code = "AAAAAAAA" });
            await inserter.AddAsync(new LowCardinalityFixedStringRow { Id = 2, Code = "BBBBBBBB" });
            await inserter.AddAsync(new LowCardinalityFixedStringRow { Id = 3, Code = "AAAAAAAA" });
            await inserter.AddAsync(new LowCardinalityFixedStringRow { Id = 4, Code = "CCCCCCCC" });
            await inserter.AddAsync(new LowCardinalityFixedStringRow { Id = 5, Code = "AAAAAAAA" });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(5, count);

            // FixedString reads back as byte[]
            var results = new List<byte[]>();
            await foreach (var row in connection.QueryAsync($"SELECT code FROM {tableName} ORDER BY id"))
            {
                results.Add(row.GetFieldValue<byte[]>("code"));
            }

            Assert.Equal(5, results.Count);
            // Each result should be 8 bytes
            Assert.All(results, r => Assert.Equal(8, r.Length));

            // First byte of row 1 should be 'A'
            Assert.Equal((byte)'A', results[0][0]);
            // First byte of row 2 should be 'B'
            Assert.Equal((byte)'B', results[1][0]);
            // Row 3 should match row 1 (same value through LowCardinality)
            Assert.Equal(results[0], results[2]);
        }
        finally
        {
            await using var cleanup = new ClickHouseConnection(_fixture.ConnectionString);
            await cleanup.OpenAsync();
            await cleanup.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region LowCardinality Overflow UInt8 to UInt16

    [Fact]
    public async Task BulkInsert_LowCardinality_Overflow_UInt8ToUInt16()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                val LowCardinality(String)
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 500 };
            await using var inserter = connection.CreateBulkInserter<LowCardinalityOverflowRow>(tableName, options);
            await inserter.InitAsync();

            // Insert 300 rows with unique strings to force index type promotion
            // from UInt8 (max 256 unique values) to UInt16
            for (int i = 0; i < 300; i++)
            {
                await inserter.AddAsync(new LowCardinalityOverflowRow
                {
                    Id = i,
                    Val = $"unique_value_{i}"
                });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(300, count);

            var distinctCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count(DISTINCT val) FROM {tableName}");
            Assert.Equal(300, distinctCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region LowCardinality Overflow UInt16 to UInt32

    [Fact]
    public async Task BulkInsert_LowCardinality_Overflow_UInt16ToUInt32()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                val LowCardinality(String)
            ) ENGINE = Memory");

        try
        {
            var options = new BulkInsertOptions { BatchSize = 80000 };
            await using var inserter = connection.CreateBulkInserter<LowCardinalityOverflowRow>(tableName, options);
            await inserter.InitAsync();

            // Insert 70000 rows with unique strings to force index type promotion
            // from UInt16 (max 65536 unique values) to UInt32
            for (int i = 0; i < 70000; i++)
            {
                await inserter.AddAsync(new LowCardinalityOverflowRow
                {
                    Id = i,
                    Val = $"unique_value_{i}"
                });
            }

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(70000, count);

            var distinctCount = await connection.ExecuteScalarAsync<long>(
                $"SELECT count(DISTINCT val) FROM {tableName}");
            Assert.Equal(70000, distinctCount);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    #endregion

    #region Nested Type

    [Fact]
    public async Task BulkInsert_NestedType_RoundTrips()
    {
        var tableName = $"test_composite_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Nested is syntactic sugar for parallel arrays
        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id Int32,
                a Array(Int32),
                b Array(String)
            ) ENGINE = Memory");

        try
        {
            await using var inserter = connection.CreateBulkInserter<NestedTypeRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new NestedTypeRow { Id = 1, A = new[] { 10, 20, 30 }, B = new[] { "x", "y", "z" } });
            await inserter.AddAsync(new NestedTypeRow { Id = 2, A = Array.Empty<int>(), B = Array.Empty<string>() });
            await inserter.AddAsync(new NestedTypeRow { Id = 3, A = new[] { 100 }, B = new[] { "only" } });

            await inserter.CompleteAsync();

            var count = await connection.ExecuteScalarAsync<long>($"SELECT count() FROM {tableName}");
            Assert.Equal(3, count);

            var results = new List<(int id, int[] a, string[] b)>();
            await foreach (var row in connection.QueryAsync($"SELECT id, a, b FROM {tableName} ORDER BY id"))
            {
                var id = row.GetFieldValue<int>("id");
                var a = (int[])row.GetFieldValue<object>("a");
                var b = (string[])row.GetFieldValue<object>("b");
                results.Add((id, a, b));
            }

            Assert.Equal(3, results.Count);

            // Row 1
            Assert.Equal(new[] { 10, 20, 30 }, results[0].a);
            Assert.Equal(new[] { "x", "y", "z" }, results[0].b);

            // Row 2 - empty arrays
            Assert.Empty(results[1].a);
            Assert.Empty(results[1].b);

            // Row 3
            Assert.Equal(new[] { 100 }, results[2].a);
            Assert.Equal(new[] { "only" }, results[2].b);
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

    private class ArrayOfArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "nested_arr", Order = 1)]
        public int[][] NestedArr { get; set; } = Array.Empty<int[]>();
    }

    private class ArrayOfNullableRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "tags", Order = 1)]
        public string?[] Tags { get; set; } = Array.Empty<string?>();
    }

    private class MapWithNullableValueRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "metadata", Order = 1)]
        public Dictionary<string, int?> Metadata { get; set; } = new();
    }

    private class MapOfArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "data", Order = 1)]
        public Dictionary<string, int[]> Data { get; set; } = new();
    }

    private class TupleWithNullableRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "pair", Order = 1)]
        public object?[] Pair { get; set; } = Array.Empty<object?>();
    }

    private class ArrayOfTupleRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "items", Order = 1)]
        public object[][] Items { get; set; } = Array.Empty<object[]>();
    }

    private class LowCardinalityFixedStringRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "code", Order = 1)]
        public string Code { get; set; } = string.Empty;
    }

    private class NestedTypeRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "a", Order = 1)]
        public int[] A { get; set; } = Array.Empty<int>();

        [ClickHouseColumn(Name = "b", Order = 2)]
        public string[] B { get; set; } = Array.Empty<string>();
    }

    private class LowCardinalityOverflowRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "val", Order = 1)]
        public string Val { get; set; } = string.Empty;
    }

    #endregion
}

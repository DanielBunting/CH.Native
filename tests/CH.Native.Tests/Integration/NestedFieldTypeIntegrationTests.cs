using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Cluster: rich FIELD TYPES inside a Nested column. Prior Nested tests only used
/// String/Int32/Int64/Float64. These exercise field types with distinct wire elements
/// under the shared-offsets layout: LowCardinality (emits a prefix), Nullable (null
/// bitmap), Array (its own offsets nested under the shared block), DateTime64 and
/// Decimal (parameterized encodings). Bulk-insert (flatten_nested=0) + whole-column read.
/// </summary>
[Collection("ClickHouse")]
public class NestedFieldTypeIntegrationTests
{
    private readonly ClickHouseFixture _fixture;

    public NestedFieldTypeIntegrationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private class NestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(Name = "n", Order = 1)]
        public object[] N { get; set; } = Array.Empty<object>();
    }

    // Inserts rows into a `n <nestedType>` table (flatten_nested=0) and returns the
    // whole-column read-back of n per row.
    private async Task<List<object[]>> InsertAndReadWhole(string nestedType, params object[][] nestedValues)
    {
        var table = $"nfield_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync("SET flatten_nested = 0");
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, n {nestedType}) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<NestedRow>(table);
            await inserter.InitAsync();
            for (int i = 0; i < nestedValues.Length; i++)
                await inserter.AddAsync(new NestedRow { Id = i + 1, N = nestedValues[i] });
            await inserter.CompleteAsync();

            await using var readConn = new ClickHouseConnection(_fixture.ConnectionString);
            await readConn.OpenAsync();
            await readConn.ExecuteNonQueryAsync("SET flatten_nested = 0");

            var rows = new List<object[]>();
            await foreach (var row in readConn.QueryStreamAsync($"SELECT n FROM {table} ORDER BY id"))
                rows.Add((object[])row.GetFieldValue<object>("n"));
            return rows;
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Nested_LowCardinalityStringField_RoundTrips()
    {
        var rows = await InsertAndReadWhole(
            "Nested(tag LowCardinality(String), v Int32)",
            new object[] { new[] { "red", "red", "blue" }, new[] { 1, 1, 2 } },
            new object[] { Array.Empty<string>(), Array.Empty<int>() });

        Assert.Equal(2, rows.Count);
        Assert.Equal(new[] { "red", "red", "blue" }, (string[])rows[0][0]);
        Assert.Equal(new[] { 1, 1, 2 }, (int[])rows[0][1]);
        Assert.Empty((string[])rows[1][0]);
    }

    [Fact]
    public async Task Nested_NullableIntField_RoundTrips()
    {
        var rows = await InsertAndReadWhole(
            "Nested(k String, v Nullable(Int32))",
            new object[] { new[] { "a", "b", "c" }, new int?[] { 10, null, 30 } },
            new object[] { Array.Empty<string>(), Array.Empty<int?>() });

        Assert.Equal(2, rows.Count);
        Assert.Equal(new[] { "a", "b", "c" }, (string[])rows[0][0]);
        Assert.Equal(new int?[] { 10, null, 30 }, (int?[])rows[0][1]);
    }

    [Fact]
    public async Task Nested_DateTime64Field_RoundTrips()
    {
        var t0 = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var rows = await InsertAndReadWhole(
            "Nested(ts DateTime64(3, 'UTC'), v Int32)",
            new object[] { new[] { t0, t0.AddMilliseconds(500) }, new[] { 1, 2 } });

        Assert.Single(rows);
        var ts = (DateTime[])rows[0][0];
        Assert.Equal(2, ts.Length);
        Assert.Equal(t0, ts[0]);
        Assert.Equal(t0.AddMilliseconds(500), ts[1]);
    }

    [Fact]
    public async Task Nested_DecimalField_RoundTrips()
    {
        var rows = await InsertAndReadWhole(
            "Nested(price Decimal(10, 2), qty Int32)",
            new object[] { new[] { 19.99m, 0.01m }, new[] { 3, 5 } });

        Assert.Single(rows);
        Assert.Equal(new[] { 19.99m, 0.01m }, (decimal[])rows[0][0]);
        Assert.Equal(new[] { 3, 5 }, (int[])rows[0][1]);
    }

    [Fact]
    public async Task Nested_ArrayField_RoundTrips()
    {
        // A Nested field that is itself an Array: the field's per-row value is an
        // array-of-arrays (the field has its own offsets nested under the shared block).
        var rows = await InsertAndReadWhole(
            "Nested(vals Array(Int32), label String)",
            new object[] { new[] { new[] { 1, 2 }, new[] { 3 } }, new[] { "x", "y" } },
            new object[] { Array.Empty<int[]>(), Array.Empty<string>() });

        Assert.Equal(2, rows.Count);
        var vals = (int[][])rows[0][0];
        Assert.Equal(2, vals.Length);
        Assert.Equal(new[] { 1, 2 }, vals[0]);
        Assert.Equal(new[] { 3 }, vals[1]);
        Assert.Equal(new[] { "x", "y" }, (string[])rows[0][1]);
    }
}

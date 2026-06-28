using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Cluster: edge VALUES inside composites. These value shapes (special floats, empty
/// strings, NUL/unicode) are tested as scalars elsewhere but never inside Array / Map /
/// Nested, where the surrounding offsets/bitmaps could interact with them.
/// </summary>
[Collection("ClickHouse")]
public class CompositeEdgeValueIntegrationTests
{
    private readonly ClickHouseFixture _fixture;

    public CompositeEdgeValueIntegrationTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private class FloatArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "vals", Order = 1)] public double[] Vals { get; set; } = Array.Empty<double>();
    }

    [Fact]
    public async Task ArrayFloat64_SpecialValues_RoundTrip()
    {
        var table = $"edge_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, vals Array(Float64)) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<FloatArrayRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new FloatArrayRow
            {
                Id = 1,
                Vals = new[] { double.NaN, double.PositiveInfinity, double.NegativeInfinity, -0.0, 1.5 },
            });
            await inserter.CompleteAsync();

            double[]? read = null;
            await foreach (var row in conn.QueryStreamAsync($"SELECT vals FROM {table}"))
                read = (double[])row.GetFieldValue<object>("vals");

            Assert.NotNull(read);
            Assert.Equal(5, read!.Length);
            Assert.True(double.IsNaN(read[0]));
            Assert.True(double.IsPositiveInfinity(read[1]));
            Assert.True(double.IsNegativeInfinity(read[2]));
            Assert.True(read[3] == 0.0 && double.IsNegative(read[3])); // -0.0 preserved
            Assert.Equal(1.5, read[4]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class StringArrayRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "vals", Order = 1)] public string[] Vals { get; set; } = Array.Empty<string>();
    }

    [Fact]
    public async Task ArrayString_EmptyAndUnicodeAndNul_RoundTrip()
    {
        var table = $"edge_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, vals Array(String)) ENGINE = Memory");
        try
        {
            var values = new[] { "", "héllo", "emoji😀", "tab\tend", "nul\0byte", "" };
            await using var inserter = conn.CreateBulkInserter<StringArrayRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new StringArrayRow { Id = 1, Vals = values });
            await inserter.CompleteAsync();

            string[]? read = null;
            await foreach (var row in conn.QueryStreamAsync($"SELECT vals FROM {table}"))
                read = (string[])row.GetFieldValue<object>("vals");

            Assert.Equal(values, read);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class MapRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "m", Order = 1)] public Dictionary<string, string> M { get; set; } = new();
    }

    [Fact]
    public async Task MapStringString_EmptyKeyAndValue_RoundTrip()
    {
        var table = $"edge_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, m Map(String, String)) ENGINE = Memory");
        try
        {
            var map = new Dictionary<string, string> { [""] = "emptykey", ["k"] = "", ["u"] = "💡" };
            await using var inserter = conn.CreateBulkInserter<MapRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new MapRow { Id = 1, M = map });
            await inserter.CompleteAsync();

            Dictionary<string, string>? read = null;
            await foreach (var row in conn.QueryStreamAsync($"SELECT m FROM {table}"))
                read = (Dictionary<string, string>)row.GetFieldValue<object>("m");

            Assert.NotNull(read);
            Assert.Equal("emptykey", read![""]);
            Assert.Equal("", read["k"]);
            Assert.Equal("💡", read["u"]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    private class NestedRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "n", Order = 1)] public object[] N { get; set; } = Array.Empty<object>();
    }

    [Fact]
    public async Task NestedFloatField_SpecialValues_RoundTrip()
    {
        var table = $"edge_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync("SET flatten_nested = 0");
        await conn.ExecuteNonQueryAsync($"CREATE TABLE {table} (id Int32, n Nested(score Float64, label String)) ENGINE = Memory");
        try
        {
            await using var inserter = conn.CreateBulkInserter<NestedRow>(table);
            await inserter.InitAsync();
            await inserter.AddAsync(new NestedRow
            {
                Id = 1,
                N = new object[]
                {
                    new[] { double.NaN, double.PositiveInfinity },
                    new[] { "", "x" },
                },
            });
            await inserter.CompleteAsync();

            object[]? n = null;
            await foreach (var row in conn.QueryStreamAsync($"SELECT n FROM {table}"))
                n = (object[])row.GetFieldValue<object>("n");

            Assert.NotNull(n);
            var scores = (double[])n![0];
            Assert.True(double.IsNaN(scores[0]));
            Assert.True(double.IsPositiveInfinity(scores[1]));
            Assert.Equal(new[] { "", "x" }, (string[])n[1]);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

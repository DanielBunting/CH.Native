using System.Collections;
using System.Linq;
using CH.Native.Connection;
using CH.Native.Data.Dynamic;
using CH.Native.Numerics;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Writing per-scalar / composite values into a <c>Dynamic</c> column, ported from the driver's
/// DynamicTests Write_* cases. CH.Native writes a Dynamic value as an explicit
/// <see cref="ClickHouseDynamic"/> wrapper (discriminator 0, boxed value, declared type name) — the
/// writer dispatches on <see cref="ClickHouseDynamic.DeclaredTypeName"/>, not the CLR type — and reads
/// it back as a <see cref="ClickHouseDynamic"/> (not the unwrapped CLR value).
/// </summary>
[Collection("ClickHouse")]
public class DynamicWriteTests
{
    private readonly ClickHouseFixture _fixture;

    public DynamicWriteTests(ClickHouseFixture fixture) => _fixture = fixture;

    private async Task<ClickHouseDynamic> RoundTripAsync(object? dynamicValue)
    {
        var table = $"dyn_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        // Dynamic is behind an experimental flag on some server versions; ignore if unknown.
        try { await connection.ExecuteNonQueryAsync("SET allow_experimental_dynamic_type = 1"); }
        catch { /* setting absent on this version */ }

        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt32, value Dynamic) ENGINE = Memory");
        try
        {
            await connection.BulkInsertAsync(table, new[] { "id", "value" },
                new[] { new object?[] { 1u, dynamicValue } });

            await foreach (var row in connection.QueryStreamAsync($"SELECT value FROM {table}"))
                return row.GetFieldValue<ClickHouseDynamic>("value");

            throw new Xunit.Sdk.XunitException("no rows returned");
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task Write_Int32()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, 42, "Int32"));
        Assert.Equal("Int32", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<int>(out var v));
        Assert.Equal(42, v);
    }

    [Fact]
    public async Task Write_Int64()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, 9_223_372_036_854_775_807L, "Int64"));
        Assert.Equal("Int64", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<long>(out var v));
        Assert.Equal(9_223_372_036_854_775_807L, v);
    }

    [Fact]
    public async Task Write_Float64()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, 3.14159, "Float64"));
        Assert.Equal("Float64", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<double>(out var v));
        Assert.Equal(3.14159, v, 10);
    }

    [Fact]
    public async Task Write_String()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, "hello world", "String"));
        Assert.Equal("String", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<string>(out var v));
        Assert.Equal("hello world", v);
    }

    [Fact]
    public async Task Write_Bool()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, true, "Bool"));
        Assert.Equal("Bool", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<bool>(out var v));
        Assert.True(v);
    }

    [Fact]
    public async Task Write_Guid()
    {
        var guid = Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0");
        var d = await RoundTripAsync(new ClickHouseDynamic(0, guid, "UUID"));
        Assert.Equal("UUID", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<Guid>(out var v));
        Assert.Equal(guid, v);
    }

    [Fact]
    public async Task Write_DateTime()
    {
        var dt = new DateTime(2024, 6, 15, 10, 30, 45, DateTimeKind.Unspecified);
        var d = await RoundTripAsync(new ClickHouseDynamic(0, dt, "DateTime"));
        Assert.Equal("DateTime", d.DeclaredTypeName);
        Assert.True(d.TryGetAs<DateTime>(out var v));
        Assert.Equal(dt, v); // DateTime.Equals compares ticks, ignoring Kind
    }

    [Fact]
    public async Task Write_Decimal()
    {
        ClickHouseDecimal value = 123.456789m;
        var d = await RoundTripAsync(new ClickHouseDynamic(0, value, "Decimal128(6)"));
        // Server normalizes Decimal128(6) to Decimal(38, 6).
        Assert.StartsWith("Decimal", d.DeclaredTypeName);
        Assert.NotNull(d.Value);
        // Read-back CLR type may be decimal or ClickHouseDecimal; Convert handles both (IConvertible).
        Assert.Equal(123.456789m, Convert.ToDecimal(d.Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task Write_IntArray()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, new[] { 1, 2, 3, 4, 5 }, "Array(Int32)"));
        Assert.Equal("Array(Int32)", d.DeclaredTypeName);
        var items = ((IEnumerable)d.Value!).Cast<object>().Select(Convert.ToInt32).ToArray();
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, items);
    }

    [Fact]
    public async Task Write_StringList()
    {
        var d = await RoundTripAsync(new ClickHouseDynamic(0, new List<string> { "a", "b", "c" }, "Array(String)"));
        Assert.Equal("Array(String)", d.DeclaredTypeName);
        var items = ((IEnumerable)d.Value!).Cast<object>().Select(o => (string)o).ToArray();
        Assert.Equal(new[] { "a", "b", "c" }, items);
    }

    [Fact]
    public async Task Write_Dictionary()
    {
        var map = new Dictionary<string, int> { ["one"] = 1, ["two"] = 2 };
        var d = await RoundTripAsync(new ClickHouseDynamic(0, map, "Map(String, Int32)"));
        Assert.StartsWith("Map(", d.DeclaredTypeName);
        Assert.NotNull(d.Value);
    }

    [Fact]
    public async Task Write_Null()
    {
        var d = await RoundTripAsync(ClickHouseDynamic.Null);
        Assert.True(d.IsNull);
    }

    [Fact]
    public async Task Write_MixedTypesInSameColumn()
    {
        var table = $"dyn_{Guid.NewGuid():N}";
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        try { await connection.ExecuteNonQueryAsync("SET allow_experimental_dynamic_type = 1"); }
        catch { /* setting absent on this version */ }

        await connection.ExecuteNonQueryAsync($"CREATE TABLE {table} (id UInt32, value Dynamic) ENGINE = Memory");
        try
        {
            await connection.BulkInsertAsync(table, new[] { "id", "value" },
                new[]
                {
                    new object?[] { 1u, new ClickHouseDynamic(0, 42, "Int32") },
                    new object?[] { 2u, new ClickHouseDynamic(0, "hello", "String") },
                    new object?[] { 3u, new ClickHouseDynamic(0, 3.14, "Float64") },
                    new object?[] { 4u, new ClickHouseDynamic(0, true, "Bool") },
                });

            var byId = new Dictionary<uint, ClickHouseDynamic>();
            await using var reader = await connection.ExecuteReaderAsync($"SELECT id, value FROM {table} ORDER BY id");
            while (await reader.ReadAsync())
                byId[reader.GetFieldValue<uint>(0)] = reader.GetFieldValue<ClickHouseDynamic>(1);

            Assert.Equal("Int32", byId[1].DeclaredTypeName);
            Assert.Equal("String", byId[2].DeclaredTypeName);
            Assert.Equal("Float64", byId[3].DeclaredTypeName);
            Assert.Equal("Bool", byId[4].DeclaredTypeName);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }
}

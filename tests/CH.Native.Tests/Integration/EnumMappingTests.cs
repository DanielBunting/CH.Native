using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// End-to-end coverage for mapping result columns onto CLR <see cref="System.Enum"/>
/// properties through <c>QueryStreamAsync&lt;T&gt;</c>. This is the
/// <c>TypeMapper.ConvertValue</c> slow path (the reason the typed-accessor fast path
/// has a fallback at all): the rewrite's own docs cite <c>Enum.ToObject</c>/<c>Enum.Parse</c>
/// as the motivating case, yet no test exercised it. ClickHouse <c>Enum8</c>/<c>Enum16</c>
/// are read as the numeric <c>sbyte</c>/<c>short</c> (see Enum8ColumnReader), so an enum
/// property hits <c>Enum.ToObject</c>; a <c>String</c> column carrying the member name
/// hits <c>Enum.Parse</c>.
/// </summary>
[Collection("ClickHouse")]
public class EnumMappingTests
{
    private readonly ClickHouseFixture _fixture;

    public EnumMappingTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    public enum Status
    {
        Inactive = 0,
        Active = 1,
        Pending = 2,
    }

    public enum Priority : short
    {
        Low = 10,
        High = 4000, // exceeds sbyte to force Enum16
    }

    private class StatusRow
    {
        public Status Value { get; set; }
    }

    private class PriorityRow
    {
        public Priority Value { get; set; }
    }

    private class NullableStatusRow
    {
        public Status? Value { get; set; }
    }

    private async Task<ClickHouseConnection> OpenAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    private async Task<T> SingleAsync<T>(ClickHouseConnection conn, string sql) where T : new()
    {
        var rows = new List<T>();
        await foreach (var r in conn.QueryStreamAsync<T>(sql))
            rows.Add(r);
        return Assert.Single(rows);
    }

    [Fact]
    public async Task Enum8Column_MapsViaToObject()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StatusRow>(conn,
            "SELECT CAST(2 AS Enum8('Inactive' = 0, 'Active' = 1, 'Pending' = 2)) AS Value");
        Assert.Equal(Status.Pending, r.Value);
    }

    [Fact]
    public async Task IntegerColumn_MapsToEnumViaToObject()
    {
        // toInt8 → sbyte → Enum.ToObject for the int-backed enum.
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StatusRow>(conn, "SELECT toInt8(1) AS Value");
        Assert.Equal(Status.Active, r.Value);
    }

    [Fact]
    public async Task Enum16Column_MapsToShortBackedEnum()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<PriorityRow>(conn,
            "SELECT CAST(4000 AS Enum16('Low' = 10, 'High' = 4000)) AS Value");
        Assert.Equal(Priority.High, r.Value);
    }

    [Fact]
    public async Task StringColumn_MapsToEnumViaParse()
    {
        // value arrives as a string → Enum.Parse branch.
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StatusRow>(conn, "SELECT 'Pending' AS Value");
        Assert.Equal(Status.Pending, r.Value);
    }

    [Fact]
    public async Task NullableEnum_NullValue_MapsToNull()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableStatusRow>(conn,
            "SELECT CAST(NULL AS Nullable(Int8)) AS Value");
        Assert.Null(r.Value);
    }

    [Fact]
    public async Task NullableEnum_PresentValue_MapsThroughUnderlyingThenEnum()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableStatusRow>(conn,
            "SELECT CAST(2 AS Nullable(Int8)) AS Value");
        Assert.Equal(Status.Pending, r.Value);
    }
}

using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins the observable behaviour of <c>TypeMapper&lt;T&gt;</c> via the public
/// <c>QueryStreamAsync&lt;T&gt;</c> path. Added before the typed-accessor
/// refactor (Part A of the Dapper allocation gap plan) so any value-shape
/// regression is caught immediately.
/// </summary>
[Collection("ClickHouse")]
public class TypeMapperRoundTripTests
{
    private readonly ClickHouseFixture _fixture;

    public TypeMapperRoundTripTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task<ClickHouseConnection> OpenAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    // ------------------------------------------------------------------
    // Primitive value-type round trips. One test per primitive so failures
    // pinpoint the bad type.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Primitive_Bool()
    {
        await using var conn = await OpenAsync();
        var rows = new List<BoolRow>();
        await foreach (var r in conn.QueryStreamAsync<BoolRow>("SELECT true AS Value"))
            rows.Add(r);
        Assert.Single(rows);
        Assert.True(rows[0].Value);
    }

    [Fact]
    public async Task Primitive_SByte()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<SByteRow>(conn, "SELECT toInt8(-7) AS Value");
        Assert.Equal((sbyte)-7, r.Value);
    }

    [Fact]
    public async Task Primitive_Byte()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<ByteRow>(conn, "SELECT toUInt8(200) AS Value");
        Assert.Equal((byte)200, r.Value);
    }

    [Fact]
    public async Task Primitive_Short()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<ShortRow>(conn, "SELECT toInt16(-1234) AS Value");
        Assert.Equal((short)-1234, r.Value);
    }

    [Fact]
    public async Task Primitive_UShort()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<UShortRow>(conn, "SELECT toUInt16(54321) AS Value");
        Assert.Equal((ushort)54321, r.Value);
    }

    [Fact]
    public async Task Primitive_Int()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<IntRow>(conn, "SELECT toInt32(-987654) AS Value");
        Assert.Equal(-987654, r.Value);
    }

    [Fact]
    public async Task Primitive_UInt()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<UIntRow>(conn, "SELECT toUInt32(3000000000) AS Value");
        Assert.Equal(3000000000u, r.Value);
    }

    [Fact]
    public async Task Primitive_Long()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<LongRow>(conn, "SELECT toInt64(-9000000000000000000) AS Value");
        Assert.Equal(-9000000000000000000L, r.Value);
    }

    [Fact]
    public async Task Primitive_ULong()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<ULongRow>(conn, "SELECT toUInt64(18000000000000000000) AS Value");
        Assert.Equal(18000000000000000000UL, r.Value);
    }

    [Fact]
    public async Task Primitive_Float()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<FloatRow>(conn, "SELECT toFloat32(3.5) AS Value");
        Assert.Equal(3.5f, r.Value);
    }

    [Fact]
    public async Task Primitive_Double()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<DoubleRow>(conn, "SELECT toFloat64(2.71828) AS Value");
        Assert.Equal(2.71828, r.Value, precision: 9);
    }

    [Fact]
    public async Task Primitive_Decimal()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<DecimalRow>(conn, "SELECT toDecimal64(1234.5678, 4) AS Value");
        Assert.Equal(1234.5678m, r.Value);
    }

    [Fact]
    public async Task Primitive_String()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StringRow>(conn, "SELECT 'hello clickhouse' AS Value");
        Assert.Equal("hello clickhouse", r.Value);
    }

    [Fact]
    public async Task Primitive_Guid()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<GuidRow>(conn, "SELECT toUUID('12345678-1234-1234-1234-123456789012') AS Value");
        Assert.Equal(Guid.Parse("12345678-1234-1234-1234-123456789012"), r.Value);
    }

    [Fact]
    public async Task Primitive_DateTime()
    {
        // Server-default timezone DateTime — column reader emits DateTime directly.
        // See Primitive_DateTime_TimezoneAware below for the typed-timezone column
        // case, which currently loses data and is fixed by the Part A refactor.
        await using var conn = await OpenAsync();
        var r = await SingleAsync<DateTimeRow>(conn, "SELECT toDateTime('2026-05-28 14:30:00') AS Value");
        Assert.Equal(new DateTime(2026, 5, 28, 14, 30, 0), r.Value);
    }

    [Fact]
    public async Task Primitive_DateTime_TimezoneAware()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<DateTimeRow>(conn,
            "SELECT toDateTime('2026-05-28 14:30:00', 'UTC') AS Value");
        Assert.Equal(new DateTime(2026, 5, 28, 14, 30, 0, DateTimeKind.Utc), r.Value);
    }

    [Fact]
    public async Task Primitive_DateOnly()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<DateOnlyRow>(conn, "SELECT toDate('2026-05-28') AS Value");
        Assert.Equal(new DateOnly(2026, 5, 28), r.Value);
    }

    // ------------------------------------------------------------------
    // Nullable variants — both the null and non-null branch.
    // ------------------------------------------------------------------

    [Fact]
    public async Task Nullable_Int_Value()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableIntRow>(conn,
            "SELECT CAST(42 AS Nullable(Int32)) AS Value");
        Assert.Equal(42, r.Value);
    }

    [Fact]
    public async Task Nullable_Int_Null()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableIntRow>(conn,
            "SELECT CAST(NULL AS Nullable(Int32)) AS Value");
        Assert.Null(r.Value);
    }

    [Fact]
    public async Task Nullable_DateTime_Value()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableDateTimeRow>(conn,
            "SELECT CAST(toDateTime('2026-05-28 09:00:00') AS Nullable(DateTime)) AS Value");
        Assert.Equal(new DateTime(2026, 5, 28, 9, 0, 0), r.Value);
    }

    [Fact]
    public async Task Nullable_DateTime_Null()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<NullableDateTimeRow>(conn,
            "SELECT CAST(NULL AS Nullable(DateTime)) AS Value");
        Assert.Null(r.Value);
    }

    [Fact]
    public async Task Nullable_String_Value()
    {
        // String is a reference type; null is the canonical missing marker.
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StringRow>(conn,
            "SELECT CAST('present' AS Nullable(String)) AS Value");
        Assert.Equal("present", r.Value);
    }

    [Fact]
    public async Task Nullable_String_Null()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<StringRow>(conn,
            "SELECT CAST(NULL AS Nullable(String)) AS Value");
        Assert.Null(r.Value);
    }

    // ------------------------------------------------------------------
    // snake_case → PascalCase mapping (the ToSnakeCase fallback path).
    // ------------------------------------------------------------------

    [Fact]
    public async Task SnakeCase_ColumnNamesMapToPascalCaseProperties()
    {
        await using var conn = await OpenAsync();
        var rows = new List<SnakeRow>();
        await foreach (var r in conn.QueryStreamAsync<SnakeRow>(@"
            SELECT toInt32(1) AS user_id, 'alice' AS user_name, true AS is_active"))
            rows.Add(r);
        Assert.Single(rows);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].UserName);
        Assert.True(rows[0].IsActive);
    }

    // ------------------------------------------------------------------
    // [ClickHouseColumn(Name = ...)] rename and Ignore = true.
    // ------------------------------------------------------------------

    [Fact]
    public async Task ClickHouseColumn_Rename_HonoursAttribute()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<RenamedRow>(conn,
            "SELECT 'renamed-value' AS custom_column_name");
        Assert.Equal("renamed-value", r.MyValue);
    }

    [Fact]
    public async Task ClickHouseColumn_Ignore_SkipsProperty()
    {
        await using var conn = await OpenAsync();
        // The Ignored property has no matching column. If TypeMapper didn't
        // honour Ignore it would either throw or leave Ignored as default —
        // we just need it to leave Ignored at its default and map Kept.
        var r = await SingleAsync<IgnoreRow>(conn, "SELECT 'kept-value' AS Kept");
        Assert.Equal("kept-value", r.Kept);
        Assert.Null(r.Ignored);
    }

    // ------------------------------------------------------------------
    // Multi-block result sets — span block boundaries to confirm the
    // mapper doesn't cache per-block state incorrectly. Default ClickHouse
    // block size is 65,536 rows; 70k forces ≥2 blocks.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MultiBlock_70k_Rows_AllMappedCorrectly()
    {
        await using var conn = await OpenAsync();
        int expected = 0;
        await foreach (var r in conn.QueryStreamAsync<MultiBlockRow>(
            "SELECT toInt64(number) AS Id, concat('row_', toString(number)) AS Name " +
            "FROM numbers(70000) ORDER BY number"))
        {
            Assert.Equal(expected, r.Id);
            Assert.Equal($"row_{expected}", r.Name);
            expected++;
        }
        Assert.Equal(70000, expected);
    }

    // ------------------------------------------------------------------
    // Empty result set — yields nothing without exception.
    // ------------------------------------------------------------------

    [Fact]
    public async Task EmptyResultSet_YieldsNothing()
    {
        await using var conn = await OpenAsync();
        var rows = new List<IntRow>();
        await foreach (var r in conn.QueryStreamAsync<IntRow>(
            "SELECT toInt32(number) AS Value FROM numbers(0)"))
            rows.Add(r);
        Assert.Empty(rows);
    }

    // ------------------------------------------------------------------
    // Multi-column row materialisation — the realistic Dapper-shaped case.
    // ------------------------------------------------------------------

    [Fact]
    public async Task MultiColumn_AllFieldsPopulated()
    {
        await using var conn = await OpenAsync();
        var r = await SingleAsync<MultiColumnRow>(conn, @"
            SELECT
                toInt64(7)                                  AS Id,
                'multi-column'                              AS Name,
                toFloat64(2.5)                              AS Value,
                toDateTime('2026-05-28 12:00:00')           AS Created,
                CAST(123 AS Nullable(Int32))                AS Optional,
                toUUID('00000000-0000-0000-0000-000000000001') AS Tag");

        Assert.Equal(7L, r.Id);
        Assert.Equal("multi-column", r.Name);
        Assert.Equal(2.5, r.Value);
        Assert.Equal(new DateTime(2026, 5, 28, 12, 0, 0), r.Created);
        Assert.Equal(123, r.Optional);
        Assert.Equal(Guid.Parse("00000000-0000-0000-0000-000000000001"), r.Tag);
    }

    // ------------------------------------------------------------------
    // Helper: enumerate a stream and assert exactly one row, returning it.
    // ------------------------------------------------------------------

    private static async Task<T> SingleAsync<T>(ClickHouseConnection conn, string sql)
    {
        T? captured = default;
        int count = 0;
        await foreach (var r in conn.QueryStreamAsync<T>(sql))
        {
            captured = r;
            count++;
        }
        Assert.Equal(1, count);
        return captured!;
    }

    // ------------------------------------------------------------------
    // Row shapes. Keep them tiny — each test owns the shape it asserts.
    // ------------------------------------------------------------------

    public class BoolRow { public bool Value { get; set; } }
    public class SByteRow { public sbyte Value { get; set; } }
    public class ByteRow { public byte Value { get; set; } }
    public class ShortRow { public short Value { get; set; } }
    public class UShortRow { public ushort Value { get; set; } }
    public class IntRow { public int Value { get; set; } }
    public class UIntRow { public uint Value { get; set; } }
    public class LongRow { public long Value { get; set; } }
    public class ULongRow { public ulong Value { get; set; } }
    public class FloatRow { public float Value { get; set; } }
    public class DoubleRow { public double Value { get; set; } }
    public class DecimalRow { public decimal Value { get; set; } }
    public class StringRow { public string? Value { get; set; } }
    public class GuidRow { public Guid Value { get; set; } }
    public class DateTimeRow { public DateTime Value { get; set; } }
    public class DateOnlyRow { public DateOnly Value { get; set; } }

    public class NullableIntRow { public int? Value { get; set; } }
    public class NullableDateTimeRow { public DateTime? Value { get; set; } }

    public class SnakeRow
    {
        public int UserId { get; set; }
        public string UserName { get; set; } = "";
        public bool IsActive { get; set; }
    }

    public class RenamedRow
    {
        [ClickHouseColumn(Name = "custom_column_name")]
        public string MyValue { get; set; } = "";
    }

    public class IgnoreRow
    {
        public string Kept { get; set; } = "";

        [ClickHouseColumn(Ignore = true)]
        public string? Ignored { get; set; }
    }

    public class MultiBlockRow
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
    }

    public class MultiColumnRow
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public double Value { get; set; }
        public DateTime Created { get; set; }
        public int? Optional { get; set; }
        public Guid Tag { get; set; }
    }
}

using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using CH.Native.Adbc;
using CH.Native.Data;
using CH.Native.Numerics;
using Xunit;

namespace CH.Native.Adbc.Tests;

/// <summary>
/// Unit tests for the scalar tier of the Block -&gt; Arrow RecordBatch adapter. These build
/// <see cref="TypedBlock"/>s directly and exercise the converter/schema mapper without a server.
/// </summary>
public class ScalarConversionTests
{
    private static TypedBlock Block(string name, string type, ITypedColumn column) =>
        new()
        {
            TableName = string.Empty,
            ColumnNames = new[] { name },
            ColumnTypes = new[] { type },
            Columns = new[] { column },
        };

    private static RecordBatch Convert(TypedBlock block)
    {
        var schema = ArrowSchemaMapper.ToSchema(block.ColumnNames, block.ColumnTypes);
        return BlockRecordBatchConverter.ToRecordBatch(block, schema);
    }

    [Fact]
    public void Int32_NonNull_RoundTrips()
    {
        using var block = Block("a", "Int32", new TypedColumn<int>(new[] { 1, 2, 3 }, 3));
        using var batch = Convert(block);

        Assert.Equal(3, batch.Length);
        var arr = Assert.IsType<Int32Array>(batch.Column(0));
        Assert.Equal(1, arr.GetValue(0));
        Assert.Equal(2, arr.GetValue(1));
        Assert.Equal(3, arr.GetValue(2));
        Assert.False(batch.Schema.GetFieldByIndex(0).IsNullable);
        Assert.IsType<Int32Type>(batch.Schema.GetFieldByIndex(0).DataType);
    }

    [Fact]
    public void NullableInt32_PreservesNulls()
    {
        using var block = Block("a", "Nullable(Int32)", new TypedColumn<int?>(new int?[] { 1, null, 3 }, 3));
        using var batch = Convert(block);

        var arr = Assert.IsType<Int32Array>(batch.Column(0));
        Assert.Equal(1, arr.GetValue(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal(3, arr.GetValue(2));
        Assert.True(batch.Schema.GetFieldByIndex(0).IsNullable);
    }

    [Fact]
    public void UnsignedAndFloating_Map_To_MatchingArrowArrays()
    {
        using var u64 = Block("a", "UInt64", new TypedColumn<ulong>(new ulong[] { 1, 18446744073709551615 }, 2));
        using var u64Batch = Convert(u64);
        Assert.Equal(18446744073709551615UL, Assert.IsType<UInt64Array>(u64Batch.Column(0)).GetValue(1));

        using var f64 = Block("a", "Float64", new TypedColumn<double>(new[] { 1.5, -2.25 }, 2));
        using var f64Batch = Convert(f64);
        Assert.Equal(-2.25, Assert.IsType<DoubleArray>(f64Batch.Column(0)).GetValue(1));
    }

    [Fact]
    public void Bool_Maps_To_BooleanArray()
    {
        using var block = Block("a", "Bool", new TypedColumn<bool>(new[] { true, false }, 2));
        using var batch = Convert(block);
        var arr = Assert.IsType<BooleanArray>(batch.Column(0));
        Assert.True(arr.GetValue(0));
        Assert.False(arr.GetValue(1));
    }

    [Fact]
    public void String_And_NullableString()
    {
        using var block = Block("a", "Nullable(String)", new TypedColumn<string>(new[] { "x", null!, "z" }, 3));
        using var batch = Convert(block);
        var arr = Assert.IsType<StringArray>(batch.Column(0));
        Assert.Equal("x", arr.GetString(0));
        Assert.True(arr.IsNull(1));
        Assert.Equal("z", arr.GetString(2));
    }

    [Fact]
    public void LowCardinality_Unwraps_To_Underlying_Scalar()
    {
        // GetValue on a LowCardinality column resolves the dictionary value; here a plain string
        // column stands in, and the type string drives the (unwrapped) Arrow mapping.
        using var block = Block("a", "LowCardinality(String)", new TypedColumn<string>(new[] { "p", "q" }, 2));
        using var batch = Convert(block);
        Assert.IsType<StringType>(batch.Schema.GetFieldByIndex(0).DataType);
        Assert.Equal("q", Assert.IsType<StringArray>(batch.Column(0)).GetString(1));
    }

    [Fact]
    public void Decimal_UsesPrecisionAndScale()
    {
        var values = new[]
        {
            new ClickHouseDecimal(new BigInteger(12345), 2), // 123.45
            new ClickHouseDecimal(new BigInteger(-50), 2),   // -0.50
        };
        using var block = Block("a", "Decimal(10, 2)", new TypedColumn<ClickHouseDecimal>(values, 2));
        using var batch = Convert(block);

        var type = Assert.IsType<Decimal128Type>(batch.Schema.GetFieldByIndex(0).DataType);
        Assert.Equal(10, type.Precision);
        Assert.Equal(2, type.Scale);

        var arr = Assert.IsType<Decimal128Array>(batch.Column(0));
        Assert.Equal(123.45m, arr.GetValue(0));
        Assert.Equal(-0.50m, arr.GetValue(1));
    }

    [Fact]
    public void Date_Maps_To_Date32()
    {
        var date = new DateOnly(2026, 6, 29);
        using var block = Block("a", "Date", new TypedColumn<DateOnly>(new[] { date }, 1));
        using var batch = Convert(block);
        var arr = Assert.IsType<Date32Array>(batch.Column(0));
        Assert.Equal(date, arr.GetDateOnly(0));
    }

    [Fact]
    public void DateTime_Maps_To_Timestamp_Seconds()
    {
        var dt = new DateTime(2026, 6, 29, 12, 30, 0, DateTimeKind.Utc);
        using var block = Block("a", "DateTime", new TypedColumn<DateTime>(new[] { dt }, 1));
        using var batch = Convert(block);

        var type = Assert.IsType<TimestampType>(batch.Schema.GetFieldByIndex(0).DataType);
        Assert.Equal(TimeUnit.Second, type.Unit);
        var arr = Assert.IsType<TimestampArray>(batch.Column(0));
        Assert.Equal(new DateTimeOffset(dt), arr.GetTimestamp(0));
    }

    [Fact]
    public void Uuid_Maps_To_String()
    {
        var id = Guid.Parse("11111111-2222-3333-4444-555555555555");
        using var block = Block("a", "UUID", new TypedColumn<Guid>(new[] { id }, 1));
        using var batch = Convert(block);
        Assert.IsType<StringType>(batch.Schema.GetFieldByIndex(0).DataType);
        Assert.Equal(id.ToString(), Assert.IsType<StringArray>(batch.Column(0)).GetString(0));
    }

    [Fact]
    public void EmptyBlock_Produces_ZeroRow_Batch()
    {
        using var block = Block("a", "Int32", new TypedColumn<int>(System.Array.Empty<int>(), 0));
        using var batch = Convert(block);
        Assert.Equal(0, batch.Length);
        Assert.Single(batch.Schema.FieldsList);
    }

    [Fact]
    public void Array_Composite_Converts()
    {
        using var block = Block("a", "Array(Int32)", new TypedColumn<int[]>(new[] { new[] { 1, 2 } }, 1));
        using var batch = Convert(block);

        var list = Assert.IsType<ListArray>(batch.Column(0));
        Assert.Equal(2, list.GetValueLength(0));
        var values = Assert.IsType<Int32Array>(list.Values);
        Assert.Equal(1, values.GetValue(0));
        Assert.Equal(2, values.GetValue(1));
    }

    [Fact]
    public void UnsupportedType_Throws()
    {
        // AggregateFunction has no Arrow mapping — schema building must still reject it.
        using var block = Block("a", "AggregateFunction(sum, UInt64)",
            new TypedColumn<int>(System.Array.Empty<int>(), 0));
        var ex = Record.Exception(() => Convert(block));
        Assert.IsType<NotSupportedException>(ex);
    }
}

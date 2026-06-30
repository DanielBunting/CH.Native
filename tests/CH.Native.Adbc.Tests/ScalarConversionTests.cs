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

    // The span fast path (TypedColumn<T>/TypedColumn<T?>) must produce a byte-for-byte identical Arrow
    // array to the boxing GetValue fallback. Real-server tests only ever hit TypedColumn<T> (the fast
    // path); these feed the SAME values through both branches and assert the arrays agree, covering
    // non-null and nullable storage across an integer, a float, and the bit-packed bool builder.

    [Fact]
    public void Int32_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Int32", () => new TypedColumn<int>(new[] { 1, 2, -3, int.MinValue, int.MaxValue }, 5));

    [Fact]
    public void NullableInt32_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(Int32)", () => new TypedColumn<int?>(new int?[] { 1, null, -3, null, 7 }, 5));

    [Fact]
    public void Float64_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Float64", () => new TypedColumn<double>(new[] { 1.5, -2.25, 0d, double.MaxValue }, 4));

    [Fact]
    public void NullableFloat64_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(Float64)", () => new TypedColumn<double?>(new double?[] { 1.5, null, 3d }, 3));

    [Fact]
    public void Bool_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Bool", () => new TypedColumn<bool>(new[] { true, false, true }, 3));

    [Fact]
    public void NullableBool_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(Bool)", () => new TypedColumn<bool?>(new bool?[] { true, null, false }, 3));

    [Fact]
    public void Date32_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Date32", () => new TypedColumn<DateOnly>(
            new[] { new DateOnly(2026, 6, 29), new DateOnly(1970, 1, 1), new DateOnly(2000, 2, 29) }, 3));

    [Fact]
    public void NullableDate32_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(Date32)", () => new TypedColumn<DateOnly?>(
            new DateOnly?[] { new DateOnly(2026, 6, 29), null, new DateOnly(1999, 12, 31) }, 3));

    [Fact]
    public void DateTimeNaive_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("DateTime", () => new TypedColumn<DateTime>(
            new[] { new DateTime(2026, 6, 29, 12, 0, 0, DateTimeKind.Utc), new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) }, 2));

    [Fact]
    public void DateTimeTz_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("DateTime('UTC')", () => new TypedColumn<DateTimeOffset>(
            new[] { new DateTimeOffset(2026, 6, 29, 12, 0, 0, TimeSpan.Zero), new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.FromHours(2)) }, 2));

    [Fact]
    public void NullableDateTimeTz_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(DateTime('UTC'))", () => new TypedColumn<DateTimeOffset?>(
            new DateTimeOffset?[] { new DateTimeOffset(2026, 6, 29, 12, 0, 0, TimeSpan.Zero), null }, 2));

    [Fact]
    public void DecimalNarrow_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Decimal64(4)", () => new TypedColumn<decimal>(new[] { 12.3456m, -1.5m, 0m }, 3));

    [Fact]
    public void NullableDecimalNarrow_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Nullable(Decimal64(4))", () => new TypedColumn<decimal?>(new decimal?[] { 12.3456m, null, -0.0001m }, 3));

    [Fact]
    public void DecimalWide_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("Decimal128(6)", () => new TypedColumn<ClickHouseDecimal>(
            new[] { (ClickHouseDecimal)12.345678m, (ClickHouseDecimal)(-1m) }, 2));

    [Fact]
    public void String_FastPath_MatchesBoxingFallback()
        => AssertFastEqualsBoxed("String", () => new TypedColumn<string>(new[] { "hello", null!, "" }, 3));

    private static void AssertFastEqualsBoxed(string type, Func<ITypedColumn> make)
    {
        // Fast: the concrete TypedColumn<T> hits the span branch. Boxed: the same data behind a
        // forwarding wrapper that is NOT a TypedColumn<T>, forcing the GetValue fallback.
        using var fastBlock = Block("a", type, make());
        using var fast = Convert(fastBlock);
        using var boxedBlock = Block("a", type, new ForwardingColumn(make()));
        using var boxed = Convert(boxedBlock);

        AssertArraysEqual(fast.Column(0), boxed.Column(0));
    }

    private static void AssertArraysEqual(IArrowArray a, IArrowArray b)
    {
        var aa = (Apache.Arrow.Array)a;
        var bb = (Apache.Arrow.Array)b;
        Assert.Equal(a.GetType(), b.GetType());
        Assert.Equal(aa.Length, bb.Length);
        Assert.Equal(aa.NullCount, bb.NullCount);
        for (int i = 0; i < aa.Length; i++)
        {
            Assert.Equal(aa.IsNull(i), bb.IsNull(i));
            if (!aa.IsNull(i))
                Assert.Equal(BoxedValue(a, i), BoxedValue(b, i));
        }
    }

    private static object? BoxedValue(IArrowArray array, int i) => array switch
    {
        Int32Array x => x.GetValue(i),
        DoubleArray x => x.GetValue(i),
        BooleanArray x => x.GetValue(i),
        Date32Array x => x.GetDateOnly(i),
        TimestampArray x => x.GetTimestamp(i),
        Decimal128Array x => x.GetValue(i),
        Decimal256Array x => x.GetValue(i),
        StringArray x => x.GetString(i),
        _ => throw new NotSupportedException($"No comparison for '{array.GetType().Name}'."),
    };

    /// <summary>Wraps an <see cref="ITypedColumn"/> so it is NOT a <c>TypedColumn&lt;T&gt;</c>, forcing the converter's boxing fallback.</summary>
    private sealed class ForwardingColumn : ITypedColumn
    {
        private readonly ITypedColumn _inner;
        public ForwardingColumn(ITypedColumn inner) => _inner = inner;
        public Type ElementType => _inner.ElementType;
        public int Count => _inner.Count;
        public object? GetValue(int index) => _inner.GetValue(index);
        public bool IsNull(int index) => _inner.IsNull(index);
        public void Dispose() => _inner.Dispose();
    }
}

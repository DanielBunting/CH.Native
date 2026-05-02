using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// <see cref="NullableRefColumnWriterAllocationTests"/> exercises the ref-type
/// wrapper. This pins the value-type wrapper's bitmap layout for the all-null,
/// all-non-null, and mixed cases.
/// </summary>
public class NullableValueTypeRoundTripTests
{
    [Fact]
    public void TypeName_NamespacesUnderlying()
    {
        Assert.Equal("Nullable(Int32)",
            new NullableColumnWriter<int>(new Int32ColumnWriter()).TypeName);
    }

    [Fact]
    public void ClrType_IsNullableUnderlying()
    {
        Assert.Equal(typeof(int?),
            new NullableColumnWriter<int>(new Int32ColumnWriter()).ClrType);
    }

    [Fact]
    public void WriteColumn_AllNonNull_BitmapAllZeros()
    {
        var values = new int?[] { 1, 2, 3 };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, values);

        // First N bytes are the bitmap.
        Assert.Equal(0x00, buffer.WrittenSpan[0]);
        Assert.Equal(0x00, buffer.WrittenSpan[1]);
        Assert.Equal(0x00, buffer.WrittenSpan[2]);
    }

    [Fact]
    public void WriteColumn_AllNull_BitmapAllOnes()
    {
        var values = new int?[] { null, null, null };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, values);

        Assert.Equal(0x01, buffer.WrittenSpan[0]);
        Assert.Equal(0x01, buffer.WrittenSpan[1]);
        Assert.Equal(0x01, buffer.WrittenSpan[2]);
    }

    [Fact]
    public void RoundTrip_MixedNullable_PreservesEachSlot()
    {
        var values = new int?[] { 42, null, 99, null, 7 };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void RoundTrip_NullableInt64_LargeBlock_HitsPoolPath()
    {
        // 300 rows forces the pool path on the reader (threshold 256). Mixing
        // null slots evenly exercises both bitmap branches across the pool
        // boundary.
        const int n = 300;
        var values = new long?[n];
        for (int i = 0; i < n; i++) values[i] = (i % 2 == 0) ? null : i * 1000L;

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new NullableColumnWriter<long>(new Int64ColumnWriter()).WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new NullableColumnReader<long>(new Int64ColumnReader())
            .ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void RoundTrip_NullableDateTime()
    {
        // Regression: DateTimeColumnWriter rejects values before 1970-01-01.
        // The Nullable wrapper used to write default(DateTime) (0001-01-01)
        // for null slots, which tripped the range check. Fixed by routing the
        // wrapper through the inner writer's NullPlaceholder (UnixEpoch for
        // DateTime). Pin the wire-level round-trip so it can't regress.
        var values = new DateTime?[]
        {
            new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            null,
            new DateTime(2025, 6, 15, 12, 30, 0, DateTimeKind.Utc),
        };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new NullableColumnWriter<DateTime>(new DateTimeColumnWriter())
            .WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new NullableColumnReader<DateTime>(new DateTimeColumnReader())
            .ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void RoundTrip_NullableDateTimeOffset_WithTimezone()
    {
        // Same regression class as DateTime — DateTimeWithTimezoneColumnWriter
        // also rejects pre-epoch values, so default(DateTimeOffset) would
        // trip the guard without the NullPlaceholder substitution.
        var values = new DateTimeOffset?[]
        {
            new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero),
            null,
            new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero),
        };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new NullableColumnWriter<DateTimeOffset>(new DateTimeWithTimezoneColumnWriter("UTC"))
            .WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new NullableColumnReader<DateTimeOffset>(new DateTimeWithTimezoneColumnReader("UTC"))
            .ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Constructor_NonGeneric_RejectsMismatchedInner()
    {
        Assert.Throws<ArgumentException>(() =>
            new NullableColumnWriter<int>((IColumnWriter)new StringColumnWriter()));
    }
}

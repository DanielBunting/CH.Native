using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Date32ColumnWriterTests
{
    [Fact]
    public void TypeName_IsDate32() => Assert.Equal("Date32", new Date32ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsDateOnly() => Assert.Equal(typeof(DateOnly), new Date32ColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Epoch_EmitsZero()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Date32ColumnWriter().WriteValue(ref writer, new DateOnly(1970, 1, 1));

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan));
    }

    [Theory]
    [MemberData(nameof(BoundaryDates))]
    public void WriteColumn_RoundTripsThroughReader(DateOnly value)
    {
        var values = new[] { value };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Date32ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Date32ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(value, column[0]);
    }

    public static IEnumerable<object[]> BoundaryDates()
    {
        yield return new object[] { new DateOnly(1970, 1, 1) };
        yield return new object[] { new DateOnly(1900, 1, 1) }; // pre-epoch
        yield return new object[] { new DateOnly(2299, 12, 31) }; // ClickHouse Date32 upper bound
        yield return new object[] { new DateOnly(2024, 6, 15) };
    }

    [Fact]
    public void WriteValue_PreEpoch_EmitsNegativeDays()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Date32ColumnWriter().WriteValue(ref writer, new DateOnly(1969, 12, 31));

        var days = BinaryPrimitives.ReadInt32LittleEndian(buffer.WrittenSpan);
        Assert.Equal(-1, days);
    }

    [Fact]
    public void Registry_ResolvesDate32Writer()
    {
        Assert.IsType<Date32ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Date32"));
    }
}

using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class DateTimeColumnWriterTests
{
    [Fact]
    public void TypeName_IsDateTime() => Assert.Equal("DateTime", new DateTimeColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsDateTime() => Assert.Equal(typeof(DateTime), new DateTimeColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Epoch_EmitsZero()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new DateTimeColumnWriter().WriteValue(ref writer, new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_BeforeEpoch_Throws()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var sut = new DateTimeColumnWriter();

        ArgumentOutOfRangeException? caught = null;
        try { sut.WriteValue(ref writer, new DateTime(1969, 12, 31, 0, 0, 0, DateTimeKind.Utc)); }
        catch (ArgumentOutOfRangeException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void WriteValue_PastUInt32MaxSeconds_Throws()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var sut = new DateTimeColumnWriter();

        ArgumentOutOfRangeException? caught = null;
        try { sut.WriteValue(ref writer, new DateTime(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc)); }
        catch (ArgumentOutOfRangeException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void WriteValue_LocalKind_IsConvertedToUtc()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        var local = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var expectedSeconds = (uint)(local.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
        new DateTimeColumnWriter().WriteValue(ref writer, local);

        Assert.Equal(expectedSeconds, BinaryPrimitives.ReadUInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new[]
        {
            new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2000, 6, 15, 12, 30, 45, DateTimeKind.Utc),
            new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc), // upper bound
        };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new DateTimeColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new DateTimeColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesDateTimeWriter()
    {
        Assert.IsType<DateTimeColumnWriter>(ColumnWriterRegistry.Default.GetWriter("DateTime"));
    }
}

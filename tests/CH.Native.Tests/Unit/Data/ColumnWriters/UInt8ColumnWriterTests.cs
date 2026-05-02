using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt8ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt8() => Assert.Equal("UInt8", new UInt8ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsByte() => Assert.Equal(typeof(byte), new UInt8ColumnWriter().ClrType);

    [Theory]
    [InlineData((byte)0)]
    [InlineData((byte)1)]
    [InlineData((byte)128)]
    [InlineData(byte.MaxValue)]
    public void WriteValue_EmitsSingleByte(byte value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt8ColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(1, buffer.WrittenCount);
        Assert.Equal(value, buffer.WrittenSpan[0]);
    }

    [Fact]
    public void WriteColumn_RoundTripsThroughReader()
    {
        var values = new byte[] { 0, 1, 127, 128, 255 };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt8ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt8ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(values.Length, column.Count);
        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesUInt8Writer()
    {
        Assert.IsType<UInt8ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt8"));
    }
}

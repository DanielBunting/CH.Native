using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class BoolColumnWriterTests
{
    [Fact]
    public void TypeName_IsBool() => Assert.Equal("Bool", new BoolColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsBool() => Assert.Equal(typeof(bool), new BoolColumnWriter().ClrType);

    [Theory]
    [InlineData(false, (byte)0x00)]
    [InlineData(true, (byte)0x01)]
    public void WriteValue_EmitsSingleByte(bool value, byte expected)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new BoolColumnWriter().WriteValue(ref writer, value);

        Assert.Equal(1, buffer.WrittenCount);
        Assert.Equal(expected, buffer.WrittenSpan[0]);
    }

    [Fact]
    public void WriteColumn_MixedBlock_RoundTripsThroughReader()
    {
        var values = new[] { true, false, true, true, false };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new BoolColumnWriter().WriteColumn(ref writer, values);

        Assert.Equal(values.Length, buffer.WrittenCount);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new BoolColumnReader().ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void NonGeneric_WriteColumn_AcceptsBoxedBools()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        ((IColumnWriter)new BoolColumnWriter()).WriteColumn(ref writer, new object?[] { true, false });

        Assert.Equal(2, buffer.WrittenCount);
        Assert.Equal(0x01, buffer.WrittenSpan[0]);
        Assert.Equal(0x00, buffer.WrittenSpan[1]);
    }

    [Fact]
    public void Registry_ResolvesBoolWriter()
    {
        Assert.IsType<BoolColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Bool"));
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class BFloat16ColumnReaderTests
{
    [Fact]
    public void TypeName_IsBFloat16()
    {
        Assert.Equal("BFloat16", new BFloat16ColumnReader().TypeName);
    }

    [Fact]
    public void ClrType_IsFloat()
    {
        Assert.Equal(typeof(float), new BFloat16ColumnReader().ClrType);
    }

    [Theory]
    [InlineData((ushort)0x0000, 0.0f)]
    [InlineData((ushort)0x8000, -0.0f)]
    [InlineData((ushort)0x3F80, 1.0f)]
    [InlineData((ushort)0xBF80, -1.0f)]
    [InlineData((ushort)0x7F80, float.PositiveInfinity)]
    [InlineData((ushort)0xFF80, float.NegativeInfinity)]
    public void ReadValue_DecodesKnownBitPatterns(ushort raw, float expected)
    {
        var bytes = new byte[2];
        BitConverter.TryWriteBytes(bytes, raw);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new BFloat16ColumnReader().ReadValue(ref reader);

        Assert.Equal(BitConverter.SingleToUInt32Bits(expected), BitConverter.SingleToUInt32Bits(result));
    }

    [Fact]
    public void ReadValue_NaNBitPattern_StaysNaN()
    {
        // 0x7FC0 = high bits of a quiet NaN
        ushort raw = 0x7FC0;
        var bytes = new byte[2];
        BitConverter.TryWriteBytes(bytes, raw);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new BFloat16ColumnReader().ReadValue(ref reader);

        Assert.True(float.IsNaN(result));
    }

    [Fact]
    public void ReadTypedColumn_ReadsAllRows()
    {
        ushort[] raws = [0x3F80, 0x4000, 0xBF80, 0x4040];
        var bytes = new byte[raws.Length * 2];
        for (int i = 0; i < raws.Length; i++)
        {
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 2), raws[i]);
        }
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new BFloat16ColumnReader().ReadTypedColumn(ref reader, raws.Length);

        Assert.Equal(4, column.Count);
        Assert.Equal(1.0f, column[0]);
        Assert.Equal(2.0f, column[1]);
        Assert.Equal(-1.0f, column[2]);
        Assert.Equal(3.0f, column[3]);
    }

    [Fact]
    public void Registry_ResolvesBFloat16Reader()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("BFloat16");
        Assert.IsType<BFloat16ColumnReader>(reader);
    }
}

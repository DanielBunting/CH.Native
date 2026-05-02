using System.Buffers;
using System.Numerics;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt256ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt256() => Assert.Equal("UInt256", new UInt256ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsBigInteger() => Assert.Equal(typeof(BigInteger), new UInt256ColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Zero_EmitsThirtyTwoZeros()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt256ColumnWriter().WriteValue(ref writer, BigInteger.Zero);

        Assert.Equal(32, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0, b);
    }

    [Theory]
    [MemberData(nameof(BoundaryValues))]
    public void WriteColumn_RoundTripsThroughReader(BigInteger value)
    {
        var values = new[] { value };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt256ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt256ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(value, column[0]);
    }

    public static IEnumerable<object[]> BoundaryValues()
    {
        yield return new object[] { BigInteger.Zero };
        yield return new object[] { BigInteger.One };
        yield return new object[] { (BigInteger)ulong.MaxValue };
        yield return new object[] { (BigInteger)ulong.MaxValue + 1 };
        yield return new object[] { BigInteger.Pow(2, 200) };
        yield return new object[] { BigInteger.Pow(2, 256) - 1 }; // UInt256 max
    }

    [Fact]
    public void Registry_ResolvesUInt256Writer()
    {
        Assert.IsType<UInt256ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt256"));
    }
}

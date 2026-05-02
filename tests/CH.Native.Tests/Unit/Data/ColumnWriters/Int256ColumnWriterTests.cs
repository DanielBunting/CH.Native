using System.Buffers;
using System.Numerics;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int256ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt256() => Assert.Equal("Int256", new Int256ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsBigInteger() => Assert.Equal(typeof(BigInteger), new Int256ColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Zero_EmitsThirtyTwoZeros()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int256ColumnWriter().WriteValue(ref writer, BigInteger.Zero);

        Assert.Equal(32, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0, b);
    }

    [Fact]
    public void WriteValue_NegativeOne_SignExtendsToAllOnes()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int256ColumnWriter().WriteValue(ref writer, BigInteger.MinusOne);

        Assert.Equal(32, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0xFF, b);
    }

    [Theory]
    [MemberData(nameof(BoundaryValues))]
    public void WriteColumn_RoundTripsThroughReader(BigInteger value)
    {
        var values = new[] { value };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int256ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int256ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(value, column[0]);
    }

    public static IEnumerable<object[]> BoundaryValues()
    {
        // ClickHouse Int256 range: -(2^255) .. (2^255)-1
        yield return new object[] { BigInteger.Zero };
        yield return new object[] { BigInteger.One };
        yield return new object[] { BigInteger.MinusOne };
        yield return new object[] { (BigInteger)long.MaxValue * long.MaxValue };
        yield return new object[] { -(BigInteger)long.MaxValue * long.MaxValue };
        yield return new object[] { BigInteger.Pow(2, 200) };
        yield return new object[] { -BigInteger.Pow(2, 200) };
    }

    [Fact]
    public void Registry_ResolvesInt256Writer()
    {
        Assert.IsType<Int256ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int256"));
    }
}

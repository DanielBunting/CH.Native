using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class Int128ColumnWriterTests
{
    [Fact]
    public void TypeName_IsInt128() => Assert.Equal("Int128", new Int128ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsInt128() => Assert.Equal(typeof(Int128), new Int128ColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Zero_EmitsSixteenZeros()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int128ColumnWriter().WriteValue(ref writer, Int128.Zero);

        Assert.Equal(16, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0, b);
    }

    [Fact]
    public void WriteValue_NegativeOne_EmitsAllOnes()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int128ColumnWriter().WriteValue(ref writer, Int128.NegativeOne);

        Assert.Equal(16, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0xFF, b);
    }

    [Theory]
    [MemberData(nameof(BoundaryValues))]
    public void WriteColumn_RoundTripsThroughReader(Int128 value)
    {
        var values = new[] { value };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new Int128ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new Int128ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(value, column[0]);
    }

    public static IEnumerable<object[]> BoundaryValues()
    {
        yield return new object[] { Int128.Zero };
        yield return new object[] { Int128.MinValue };
        yield return new object[] { Int128.MaxValue };
        yield return new object[] { Int128.NegativeOne };
        yield return new object[] { (Int128)long.MaxValue + 1 }; // straddles 64-bit boundary
        yield return new object[] { (Int128)long.MinValue - 1 };
    }

    [Fact]
    public void Registry_ResolvesInt128Writer()
    {
        Assert.IsType<Int128ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("Int128"));
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class UInt128ColumnWriterTests
{
    [Fact]
    public void TypeName_IsUInt128() => Assert.Equal("UInt128", new UInt128ColumnWriter().TypeName);

    [Fact]
    public void ClrType_IsUInt128() => Assert.Equal(typeof(UInt128), new UInt128ColumnWriter().ClrType);

    [Fact]
    public void WriteValue_Zero_EmitsSixteenZeros()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt128ColumnWriter().WriteValue(ref writer, UInt128.Zero);

        Assert.Equal(16, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0, b);
    }

    [Fact]
    public void WriteValue_MaxValue_EmitsAllOnes()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt128ColumnWriter().WriteValue(ref writer, UInt128.MaxValue);

        Assert.Equal(16, buffer.WrittenCount);
        foreach (var b in buffer.WrittenSpan) Assert.Equal(0xFF, b);
    }

    [Theory]
    [MemberData(nameof(BoundaryValues))]
    public void WriteColumn_RoundTripsThroughReader(UInt128 value)
    {
        var values = new[] { value };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UInt128ColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UInt128ColumnReader().ReadTypedColumn(ref reader, values.Length);

        Assert.Equal(value, column[0]);
    }

    public static IEnumerable<object[]> BoundaryValues()
    {
        yield return new object[] { UInt128.Zero };
        yield return new object[] { UInt128.One };
        yield return new object[] { UInt128.MaxValue };
        yield return new object[] { (UInt128)ulong.MaxValue };
        yield return new object[] { (UInt128)ulong.MaxValue + 1 }; // 2^64 — straddles the boundary
    }

    [Fact]
    public void Registry_ResolvesUInt128Writer()
    {
        Assert.IsType<UInt128ColumnWriter>(ColumnWriterRegistry.Default.GetWriter("UInt128"));
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class TimeColumnWriterTests
{
    [Fact]
    public void TypeName_IsTime()
    {
        Assert.Equal("Time", new TimeColumnWriter().TypeName);
    }

    [Fact]
    public void ClrType_IsTimeOnly()
    {
        Assert.Equal(typeof(TimeOnly), new TimeColumnWriter().ClrType);
    }

    [Theory]
    [InlineData("00:00:00", 0)]
    [InlineData("01:01:01", 3661)]
    [InlineData("23:59:59", 86399)]
    public void WriteValue_EncodesSecondsSinceMidnight(string time, int expectedSeconds)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new TimeColumnWriter().WriteValue(ref writer, TimeOnly.Parse(time));

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(expectedSeconds, BitConverter.ToInt32(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_RoundTripsThroughReader()
    {
        var original = new TimeOnly(13, 37, 42);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new TimeColumnWriter().WriteValue(ref writer, original);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new TimeColumnReader().ReadValue(ref reader);

        Assert.Equal(original, result);
    }

    [Fact]
    public void Registry_ResolvesTimeWriter()
    {
        var writer = ColumnWriterRegistry.Default.GetWriter("Time");
        Assert.IsType<TimeColumnWriter>(writer);
    }
}

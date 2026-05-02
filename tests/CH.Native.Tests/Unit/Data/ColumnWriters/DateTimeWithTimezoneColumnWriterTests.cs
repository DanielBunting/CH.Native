using System.Buffers;
using System.Buffers.Binary;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

public class DateTimeWithTimezoneColumnWriterTests
{
    [Fact]
    public void TypeName_EmbedsTimezone()
    {
        Assert.Equal("DateTime('UTC')", new DateTimeWithTimezoneColumnWriter("UTC").TypeName);
        Assert.Equal("DateTime('America/New_York')", new DateTimeWithTimezoneColumnWriter("America/New_York").TypeName);
    }

    [Fact]
    public void ClrType_IsDateTimeOffset()
    {
        Assert.Equal(typeof(DateTimeOffset), new DateTimeWithTimezoneColumnWriter("UTC").ClrType);
    }

    [Fact]
    public void WriteValue_Epoch_EmitsZero()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new DateTimeWithTimezoneColumnWriter("UTC")
            .WriteValue(ref writer, DateTimeOffset.FromUnixTimeSeconds(0));

        Assert.Equal(4, buffer.WrittenCount);
        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(buffer.WrittenSpan));
    }

    [Fact]
    public void WriteValue_OffsetCarriedToUtc()
    {
        // Same instant expressed via two different offsets must serialise identically.
        var bufferA = new ArrayBufferWriter<byte>();
        var writerA = new ProtocolWriter(bufferA);
        var bufferB = new ArrayBufferWriter<byte>();
        var writerB = new ProtocolWriter(bufferB);

        var sut = new DateTimeWithTimezoneColumnWriter("UTC");
        sut.WriteValue(ref writerA, new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero));
        sut.WriteValue(ref writerB, new DateTimeOffset(2024, 1, 1, 7, 0, 0, TimeSpan.FromHours(-5)));

        Assert.Equal(bufferA.WrittenSpan.ToArray(), bufferB.WrittenSpan.ToArray());
    }

    [Fact]
    public void WriteValue_BeforeEpoch_Throws()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var sut = new DateTimeWithTimezoneColumnWriter("UTC");

        ArgumentOutOfRangeException? caught = null;
        try { sut.WriteValue(ref writer, DateTimeOffset.FromUnixTimeSeconds(-1)); }
        catch (ArgumentOutOfRangeException ex) { caught = ex; }
        Assert.NotNull(caught);
    }
}

using System.Buffers;
using System.Text;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class StringColumnReaderTests
{
    [Fact]
    public void TypeName_IsString() => Assert.Equal("String", new StringColumnReader().TypeName);

    [Fact]
    public void ClrType_IsString() => Assert.Equal(typeof(string), new StringColumnReader().ClrType);

    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("with embedded\0null")]
    [InlineData("你好世界")]
    [InlineData("🎉")]
    public void ReadValue_RoundTripsThroughWriter(string value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteValue(ref writer, value);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new StringColumnReader().ReadValue(ref reader);

        Assert.Equal(value, result);
    }

    [Fact]
    public void ReadValue_VarIntBoundary_OneByteLength()
    {
        // VarInt encodes lengths 0-127 in a single byte.
        var s = new string('a', 127);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteValue(ref writer, s);

        Assert.Equal(0x7F, buffer.WrittenSpan[0]);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(s, new StringColumnReader().ReadValue(ref reader));
    }

    [Fact]
    public void ReadValue_VarIntBoundary_TwoByteLength()
    {
        // VarInt encodes lengths 128-16383 in two bytes.
        var s = new string('b', 128);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteValue(ref writer, s);

        // First length byte has continuation bit set
        Assert.True((buffer.WrittenSpan[0] & 0x80) != 0);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(s, new StringColumnReader().ReadValue(ref reader));
    }

    [Fact]
    public void ReadColumn_OverInternThreshold_DeduplicatesRepeats()
    {
        // The reader interns when rowCount >= 100. A column of repeated
        // strings should still decode equal values (interning is internal,
        // observable only via reference equality if the impl exposes it,
        // but functional correctness must hold either way).
        var values = new string[200];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = (i % 3) switch
            {
                0 => "alpha",
                1 => "beta",
                _ => "gamma",
            };
        }

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new StringColumnReader().ReadTypedColumn(ref reader, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void ReadColumn_LargeString_DecodesCorrectly()
    {
        var s = new string('x', 100_000);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteValue(ref writer, s);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(s, new StringColumnReader().ReadValue(ref reader));
    }

    [Fact]
    public void Registry_ResolvesStringReader()
    {
        Assert.IsType<StringColumnReader>(ColumnReaderRegistry.Default.GetReader("String"));
    }

    [Fact]
    public void Utf8_MultiByteCharacters_DecodeUnchanged()
    {
        // The encoded byte length differs from the .NET string length —
        // catches a class of off-by-one bugs in length-prefix handling.
        var s = "ä§∞☃🎉";
        var byteLength = Encoding.UTF8.GetByteCount(s);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new StringColumnWriter().WriteValue(ref writer, s);

        Assert.True(buffer.WrittenCount > byteLength); // length prefix + content
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.Equal(s, new StringColumnReader().ReadValue(ref reader));
    }
}

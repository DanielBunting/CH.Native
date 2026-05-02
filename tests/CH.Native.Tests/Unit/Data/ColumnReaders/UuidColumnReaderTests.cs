using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// ClickHouse stores UUIDs with each 8-byte half byte-reversed. This is a known
/// driver footgun (Java/Python clients have all had bugs here at some point).
/// These tests pin the byte-order transform so a refactor can't silently flip
/// the endianness back.
/// </summary>
public class UuidColumnReaderTests
{
    [Fact]
    public void TypeName_IsUUID() => Assert.Equal("UUID", new UuidColumnReader().TypeName);

    [Fact]
    public void ClrType_IsGuid() => Assert.Equal(typeof(Guid), new UuidColumnReader().ClrType);

    [Fact]
    public void ReadValue_AllZeros_ReturnsEmptyGuid()
    {
        var bytes = new byte[16];
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new UuidColumnReader().ReadValue(ref reader);

        Assert.Equal(Guid.Empty, result);
    }

    [Fact]
    public void ReadValue_AllOnes_RoundTripsAllOnesGuid()
    {
        var bytes = new byte[16];
        Array.Fill(bytes, (byte)0xFF);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var result = new UuidColumnReader().ReadValue(ref reader);

        Assert.Equal(new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"), result);
    }

    [Fact]
    public void ReadValue_KnownGuid_RoundTripsThroughWriter()
    {
        // Pin the symmetry: writer encodes -> reader decodes -> original.
        // If either the reader or writer flips endianness independently, this breaks.
        var guid = new Guid("550e8400-e29b-41d4-a716-446655440000");
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        new UuidColumnWriter().WriteValue(ref writer, guid);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var result = new UuidColumnReader().ReadValue(ref reader);

        Assert.Equal(guid, result);
    }

    [Fact]
    public void ReadColumn_LargeBlock_RoundTripsAllValues()
    {
        var guids = new Guid[100];
        for (int i = 0; i < guids.Length; i++) guids[i] = Guid.NewGuid();

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new UuidColumnWriter().WriteColumn(ref writer, guids);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new UuidColumnReader().ReadTypedColumn(ref reader, guids.Length);

        for (int i = 0; i < guids.Length; i++)
            Assert.Equal(guids[i], column[i]);
    }

    [Fact]
    public void Registry_ResolvesUuidReader()
    {
        Assert.IsType<UuidColumnReader>(ColumnReaderRegistry.Default.GetReader("UUID"));
    }
}

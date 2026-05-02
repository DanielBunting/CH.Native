using System.Buffers;
using System.Net;
using CH.Native.BulkInsert;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Per-primitive extractor coverage. The existing
/// <see cref="ColumnExtractorFactoryTests"/> file covers Int32 / String /
/// FixedString / Nullable composites / IPv4 / Decimal. This adds Float64,
/// Guid, IPAddress (IPv6), DateOnly, DateTime — the primitives most
/// frequently used in real schemas — and pins the byte-level wire output.
/// </summary>
public class ColumnExtractorFactoryPrimitivesTests
{
    private class FloatRow { public double Value { get; set; } }
    private class GuidRow { public Guid Id { get; set; } }
    private class IpRow { public IPAddress? Address { get; set; } }
    private class DateOnlyRow { public DateOnly Date { get; set; } }
    private class DateTimeRow { public DateTime When { get; set; } }
    private class Int64Row { public long Value { get; set; } }
    private class BoolRow { public bool Flag { get; set; } }

    private static byte[] ExtractAndWriteSingle<T>(string columnName, string clickHouseType, T row)
        where T : new()
    {
        var property = typeof(T).GetProperty(typeof(T).GetProperties()[0].Name)!;
        var extractor = ColumnExtractorFactory.Create<T>(property, columnName, clickHouseType);
        var rows = new List<T> { row };
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows, 1);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void Float64_WritesEightLittleEndianBytes()
    {
        var bytes = ExtractAndWriteSingle("value", "Float64", new FloatRow { Value = Math.PI });

        Assert.Equal(8, bytes.Length);
        Assert.Equal(Math.PI, BitConverter.ToDouble(bytes));
    }

    [Fact]
    public void Int64_WritesEightLittleEndianBytes()
    {
        var bytes = ExtractAndWriteSingle("value", "Int64", new Int64Row { Value = -42L });

        Assert.Equal(8, bytes.Length);
        Assert.Equal(-42L, BitConverter.ToInt64(bytes));
    }

    [Fact]
    public void Bool_WritesSingleByte()
    {
        var bytesT = ExtractAndWriteSingle("flag", "Bool", new BoolRow { Flag = true });
        Assert.Equal(new byte[] { 0x01 }, bytesT);

        var bytesF = ExtractAndWriteSingle("flag", "Bool", new BoolRow { Flag = false });
        Assert.Equal(new byte[] { 0x00 }, bytesF);
    }

    [Fact]
    public void Guid_WritesSixteenBytes_WithClickHouseHalfReversal()
    {
        // ClickHouse stores UUID with each 8-byte half byte-reversed. The
        // extractor delegates to UuidColumnWriter which performs that
        // transform — pin the output length and round-trip via the matching
        // reader (the byte-order is already pinned in UuidColumnReaderTests).
        var guid = new Guid("550e8400-e29b-41d4-a716-446655440000");
        var bytes = ExtractAndWriteSingle("id", "UUID", new GuidRow { Id = guid });

        Assert.Equal(16, bytes.Length);

        // Round-trip through the matching reader.
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var roundTrip = new CH.Native.Data.ColumnReaders.UuidColumnReader().ReadValue(ref reader);
        Assert.Equal(guid, roundTrip);
    }

    [Fact]
    public void IPv6_WritesSixteenNetworkOrderBytes()
    {
        var bytes = ExtractAndWriteSingle("address", "IPv6",
            new IpRow { Address = IPAddress.Parse("::1") });

        Assert.Equal(16, bytes.Length);
        // ::1 = sixteen bytes with only the last set to 0x01
        Assert.Equal(0x01, bytes[15]);
        for (int i = 0; i < 15; i++) Assert.Equal(0x00, bytes[i]);
    }

    [Fact]
    public void DateOnly_AsDate32_WritesDaysSinceEpochAsInt32()
    {
        var bytes = ExtractAndWriteSingle("date", "Date32",
            new DateOnlyRow { Date = new DateOnly(1970, 1, 1) });

        Assert.Equal(4, bytes.Length);
        Assert.Equal(0, BitConverter.ToInt32(bytes));
    }

    [Fact]
    public void DateOnly_AsDate32_PreEpoch_WritesNegativeDays()
    {
        var bytes = ExtractAndWriteSingle("date", "Date32",
            new DateOnlyRow { Date = new DateOnly(1969, 12, 31) });

        Assert.Equal(4, bytes.Length);
        Assert.Equal(-1, BitConverter.ToInt32(bytes));
    }

    [Fact]
    public void DateTime_AsDateTime_WritesUInt32UnixSeconds()
    {
        var bytes = ExtractAndWriteSingle("when", "DateTime",
            new DateTimeRow { When = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc) });

        Assert.Equal(4, bytes.Length);
        Assert.Equal(0u, BitConverter.ToUInt32(bytes));
    }
}

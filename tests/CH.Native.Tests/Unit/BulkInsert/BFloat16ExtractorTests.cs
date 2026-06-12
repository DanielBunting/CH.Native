using System.Buffers;
using CH.Native.BulkInsert;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// The direct extractor's wire width must follow the COLUMN type, not the CLR type:
/// a float property bound to a BFloat16 column writes 2 truncated bytes per value.
/// Writing 4-byte Float32 payloads here desyncs the block stream and the server
/// rejects the insert with a misleading "Unknown BlockInfo field number" error
/// (the original BFloat16 bulk-insert wire bug, fixed 2026-06-12).
/// </summary>
public class BFloat16ExtractorTests
{
    private class FloatRow { public float Value { get; set; } }
    private class NullableFloatRow { public float? Value { get; set; } }

    private static byte[] ExtractAndWrite<T>(string clickHouseType, params T[] rows)
        where T : new()
    {
        var property = typeof(T).GetProperty("Value")!;
        var extractor = ColumnExtractorFactory.Create<T>(property, "value", clickHouseType);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        extractor.ExtractAndWrite(ref writer, rows.ToList(), rows.Length);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void BFloat16_FloatProperty_WritesTwoTruncatedBytesPerValue()
    {
        // 1.5f = 0x3FC00000 -> bfloat16 0x3FC0; 2.5f = 0x40200000 -> 0x4020
        var bytes = ExtractAndWrite("BFloat16",
            new FloatRow { Value = 1.5f }, new FloatRow { Value = 2.5f });

        Assert.Equal(4, bytes.Length);
        Assert.Equal(0x3FC0, BitConverter.ToUInt16(bytes, 0));
        Assert.Equal(0x4020, BitConverter.ToUInt16(bytes, 2));
    }

    [Fact]
    public void BFloat16_MatchesBFloat16ColumnWriterOutput()
    {
        var extractorBytes = ExtractAndWrite("BFloat16", new FloatRow { Value = 3.14159f });

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new CH.Native.Data.ColumnWriters.BFloat16ColumnWriter().WriteValue(ref writer, 3.14159f);

        Assert.Equal(buffer.WrittenSpan.ToArray(), extractorBytes);
    }

    [Fact]
    public void NullableBFloat16_WritesMaskPlusTwoBytesPerValue()
    {
        var bytes = ExtractAndWrite("Nullable(BFloat16)",
            new NullableFloatRow { Value = 1.5f }, new NullableFloatRow { Value = null });

        // Null mask (1 byte/row) + 2 bytes/value including the null slot.
        Assert.Equal(2 + 4, bytes.Length);
        Assert.Equal(0x00, bytes[0]); // not null
        Assert.Equal(0x01, bytes[1]); // null
        Assert.Equal(0x3FC0, BitConverter.ToUInt16(bytes, 2));
    }

    // Regression guard: the branch must not disturb plain Float32 extraction.
    [Fact]
    public void Float32_StillWritesFourBytesPerValue()
    {
        var bytes = ExtractAndWrite("Float32", new FloatRow { Value = 1.5f });

        Assert.Equal(4, bytes.Length);
        Assert.Equal(1.5f, BitConverter.ToSingle(bytes));
    }
}

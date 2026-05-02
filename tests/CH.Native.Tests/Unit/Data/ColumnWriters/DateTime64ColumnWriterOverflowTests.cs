using System.Buffers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix the precision-8/9 branch did <c>ticks * multiplier</c> in unchecked
/// arithmetic. <see cref="DateTime.MaxValue"/> Unix-epoch ticks ≈ 2.5 × 10¹⁷;
/// multiplying by 100 (precision 9) overflows <see cref="long.MaxValue"/>
/// (~9.2 × 10¹⁸) and silently wraps to a negative wire value. The user sees
/// no error and a round-trip produces the wrong moment.
/// </summary>
public class DateTime64ColumnWriterOverflowTests
{
    [Fact]
    public void Precision9_OverflowingDateTime_ThrowsTyped()
    {
        var writer = new DateTime64ColumnWriter(precision: 9);
        var bw = new ArrayBufferWriter<byte>();

        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc));
        });
    }

    [Fact]
    public void Precision8_OverflowingDateTime_ThrowsTyped()
    {
        var writer = new DateTime64ColumnWriter(precision: 8);
        var bw = new ArrayBufferWriter<byte>();

        // At precision 8 the multiplier is 10. DateTime.MaxValue Unix ticks ×
        // 10 still overflows long.MaxValue.
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc));
        });
    }

    [Fact]
    public void Precision9_OrdinaryDateTime_StillRoundTrips()
    {
        // Sanity: typical year-2024 timestamps are well within long range
        // even at precision 9 — the fix must not regress them.
        var writer = new DateTime64ColumnWriter(precision: 9);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc));
        Assert.Equal(8, bw.WrittenCount); // single Int64
    }
}

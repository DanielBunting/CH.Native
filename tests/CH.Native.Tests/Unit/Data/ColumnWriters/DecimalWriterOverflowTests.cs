using System.Buffers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix the Decimal32 / Decimal64 writers cast their scaled <see cref="decimal"/>
/// to <c>int</c> / <c>long</c> without overflow checking. A value whose
/// scaled mantissa exceeds the wire-type's range silently wraps; the wrong
/// magnitude lands in the column with no diagnostic.
/// </summary>
public class DecimalWriterOverflowTests
{
    [Fact]
    public void Decimal32_OverflowingValue_ThrowsTyped()
    {
        // Decimal32 wire is Int32 (~2.1 × 10⁹). Scale 0 + value of 3 × 10⁹
        // overflows the cast.
        var writer = new Decimal32ColumnWriter(scale: 0);
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, 3_000_000_000m);
        });
    }

    [Fact]
    public void Decimal64_OverflowingValue_ThrowsTyped()
    {
        // Decimal64 wire is Int64. With scale 0 a 20-digit decimal overflows.
        var writer = new Decimal64ColumnWriter(scale: 0);
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, 12_345_678_901_234_567_890m);
        });
    }

    [Fact]
    public void Decimal32_HappyPath_RoundTripsAtScaleBoundary()
    {
        var writer = new Decimal32ColumnWriter(scale: 4);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, 1234.5678m); // scaled = 12345678 fits Int32
        Assert.Equal(4, bw.WrittenCount);
    }

    [Fact]
    public void Decimal64_HappyPath_RoundTripsAtScaleBoundary()
    {
        var writer = new Decimal64ColumnWriter(scale: 6);
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, 1_000_000.000001m);
        Assert.Equal(8, bw.WrittenCount);
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Tests for the <see cref="FixedSizeColumnSkipper"/> family. The contract is purely
/// arithmetic: <c>rowCount * byteSize</c> bytes consumed, no state, no framing.
/// </summary>
public class FixedSizeColumnSkipperTests
{
    public static IEnumerable<object[]> AllFixedSizeTypes() => new[]
    {
        new object[] { "Int8", 1 }, new object[] { "UInt8", 1 }, new object[] { "Bool", 1 }, new object[] { "Enum8", 1 },
        new object[] { "Int16", 2 }, new object[] { "UInt16", 2 }, new object[] { "Date", 2 }, new object[] { "BFloat16", 2 }, new object[] { "Enum16", 2 },
        new object[] { "Int32", 4 }, new object[] { "UInt32", 4 }, new object[] { "Float32", 4 }, new object[] { "DateTime", 4 }, new object[] { "Date32", 4 }, new object[] { "IPv4", 4 }, new object[] { "Decimal32", 4 },
        new object[] { "Int64", 8 }, new object[] { "UInt64", 8 }, new object[] { "Float64", 8 }, new object[] { "DateTime64", 8 }, new object[] { "Decimal64", 8 },
        new object[] { "Int128", 16 }, new object[] { "UInt128", 16 }, new object[] { "UUID", 16 }, new object[] { "IPv6", 16 }, new object[] { "Decimal128", 16 },
        new object[] { "Int256", 32 }, new object[] { "UInt256", 32 }, new object[] { "Decimal256", 32 },
    };

    [Theory]
    [MemberData(nameof(AllFixedSizeTypes))]
    public void Skip_RowCountTimesSize_AdvancesExactly(string typeName, int byteSize)
    {
        const int rowCount = 1024;
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteBytes(new byte[rowCount * byteSize]));

        var reader = new ProtocolReader(seq);
        var skipper = ColumnSkipperRegistry.Default.GetSkipper(typeName);

        Assert.True(skipper.TrySkipColumn(ref reader, rowCount));
        Assert.Equal(0, reader.Remaining);
    }

    [Theory]
    [MemberData(nameof(AllFixedSizeTypes))]
    public void Skip_TruncatedTrailingByte_ReturnsFalse(string typeName, int byteSize)
    {
        const int rowCount = 10;
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteBytes(new byte[rowCount * byteSize - 1]));

        var reader = new ProtocolReader(seq);
        var skipper = ColumnSkipperRegistry.Default.GetSkipper(typeName);

        var positionBefore = reader.Consumed;
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount));
        Assert.Equal(positionBefore, reader.Consumed);
    }

    [Theory]
    [MemberData(nameof(AllFixedSizeTypes))]
    public void Skip_RowCountSizeProduct_GreaterThanBuffer_ReturnsFalse(string typeName, int byteSize)
    {
        // Pin current behaviour: an absurd row count is reported as "not enough bytes",
        // not an OverflowException. Cast to long means the multiply itself never
        // overflows for any byteSize <= 32, so the skipper just asks TrySkipBytes for
        // a giant count and gets false back.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteBytes(new byte[byteSize])); // one row's worth

        var reader = new ProtocolReader(seq);
        var skipper = ColumnSkipperRegistry.Default.GetSkipper(typeName);

        Assert.False(skipper.TrySkipColumn(ref reader, int.MaxValue));
    }

    [Fact]
    public void Skip_FixedString_ParameterisedSize_AdvancesExactly()
    {
        const int len = 7;
        const int rowCount = 100;
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteBytes(new byte[rowCount * len]));

        var reader = new ProtocolReader(seq);
        var skipper = new FixedStringColumnSkipper(len);

        Assert.True(skipper.TrySkipColumn(ref reader, rowCount));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void ReaderVsSkipper_Int32_ConsumeSameBytes()
    {
        var values = new[] { 1, 2, 3, int.MinValue, int.MaxValue, 0, -7 };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new Int32ColumnWriter().WriteColumn(ref w, values));

        SkipperTestBase.AssertParity(
            seq, values.Length,
            readPrefix: (ref ProtocolReader r) => { },
            readColumn: (ref ProtocolReader r, int rc) =>
            {
                using var col = new Int32ColumnReader().ReadTypedColumn(ref r, rc);
            },
            skipColumn: (ref ProtocolReader r, int rc) => new Int32ColumnSkipper().TrySkipColumn(ref r, rc));
    }

    [Fact]
    public void ReaderVsSkipper_Uuid_ConsumeSameBytes()
    {
        var values = new[] { Guid.NewGuid(), Guid.Empty, Guid.NewGuid() };
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => new UuidColumnWriter().WriteColumn(ref w, values));

        SkipperTestBase.AssertParity(
            seq, values.Length,
            readPrefix: (ref ProtocolReader r) => { },
            readColumn: (ref ProtocolReader r, int rc) =>
            {
                using var col = new UuidColumnReader().ReadTypedColumn(ref r, rc);
            },
            skipColumn: (ref ProtocolReader r, int rc) => new UuidColumnSkipper().TrySkipColumn(ref r, rc));
    }
}

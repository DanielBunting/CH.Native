using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Variant;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Contract-pinning tests for <see cref="VariantColumnWriter"/>. Like
/// <c>Dynamic</c>, ClickHouse's <c>Variant(...)</c> column type is
/// intrinsically nullable: a per-row discriminator byte of
/// <see cref="ClickHouseVariant.NullDiscriminator"/> (255) marks NULL with
/// no per-arm payload. ClickHouse rejects <c>Nullable(Variant(...))</c> at
/// DDL, so the current <c>null → ClickHouseVariant.Null</c> conversion in
/// the non-generic path is the documented, correct semantic — these tests
/// lock it in so a future refactor does not flip it to a strict throw.
/// </summary>
public class VariantColumnWriterNullTests
{
    private static IColumnWriter[] StringIntWriters() => new IColumnWriter[]
    {
        new StringColumnWriter(),
        new Int32ColumnWriter(),
    };

    [Fact]
    public void NullPlaceholder_Throws_BecauseNullableVariantIsUnreachable()
    {
        // Documents that VariantColumnWriter relies on the default-throw
        // NullPlaceholder from IColumnWriter<T>: ClickHouse rejects
        // Nullable(Variant(...)) at DDL, so NullableRefColumnWriter will
        // never wrap this writer. If a future change adds the wrap usage,
        // this test will fail and force an explicit decision.
        IColumnWriter<ClickHouseVariant> sut = new VariantColumnWriter(StringIntWriters());

        Assert.Throws<NotSupportedException>(() => _ = sut.NullPlaceholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullVariant_WritesNullDiscriminator()
    {
        // ClickHouseVariant.Null has Discriminator = 255; the writer must emit
        // it verbatim into the discriminator stream and contribute no bytes
        // to any per-arm bucket.
        var sut = new VariantColumnWriter(StringIntWriters());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[]
        {
            ClickHouseVariant.Of(0, "x"),
            ClickHouseVariant.Null,
            ClickHouseVariant.Of(1, 42),
        });

        var span = buffer.WrittenSpan;
        // 3 discriminator bytes + arm-0: VarInt(1)+'x' (2 bytes) + arm-1: Int32 (4 bytes).
        Assert.Equal(3 + 2 + 4, span.Length);

        Assert.Equal(0x00, span[0]);                              // arm 0
        Assert.Equal(ClickHouseVariant.NullDiscriminator, span[1]);
        Assert.Equal(0x01, span[2]);                              // arm 1

        // Arm-0 String bucket: 1 entry "x"
        Assert.Equal(0x01, span[3]);
        Assert.Equal((byte)'x', span[4]);

        // Arm-1 Int32 bucket: 1 entry = 42
        Assert.Equal(42, BitConverter.ToInt32(span[5..9]));
    }

    [Fact]
    public void NonGeneric_WriteColumn_NullEntry_MapsToClickHouseVariantNull()
    {
        // Pin the null → ClickHouseVariant.Null semantic. Same wire output
        // as passing ClickHouseVariant.Null through the typed path.
        IColumnWriter sut = new VariantColumnWriter(StringIntWriters());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new object?[]
        {
            ClickHouseVariant.Of(0, "x"),
            null,
        });

        var span = buffer.WrittenSpan;
        // 2 discriminator bytes + arm-0: VarInt(1)+'x' (2 bytes).
        Assert.Equal(2 + 2, span.Length);

        Assert.Equal(0x00, span[0]);
        Assert.Equal(ClickHouseVariant.NullDiscriminator, span[1]);
        Assert.Equal(0x01, span[2]);
        Assert.Equal((byte)'x', span[3]);
    }
}

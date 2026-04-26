using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Dynamic;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Contract-pinning tests for <see cref="DynamicColumnWriter"/>. Unlike
/// String/FixedString/Array/Map/Tuple, the <c>Dynamic</c> column type is
/// intrinsically nullable in ClickHouse: its wire format reserves a
/// dedicated discriminator index for NULL rows and ClickHouse rejects
/// <c>Nullable(Dynamic)</c> at DDL. The current
/// <c>null → ClickHouseDynamic.Null</c> conversion in the non-generic
/// path is therefore the documented, correct semantic — these tests lock
/// it in so a future refactor does not accidentally flip it to a strict
/// throw (which would diverge from ClickHouse's data model).
/// </summary>
public class DynamicColumnWriterNullTests
{
    private static ColumnWriterFactory WriterFactory() => new(ColumnWriterRegistry.Default);

    [Fact]
    public void NullPlaceholder_Throws_BecauseNullableDynamicIsUnreachable()
    {
        // Documents that DynamicColumnWriter relies on the default-throw
        // NullPlaceholder from IColumnWriter<T>: ClickHouse rejects
        // Nullable(Dynamic) at DDL, so NullableRefColumnWriter will never
        // wrap this writer. If a future change adds the wrap usage, this
        // test will fail and force an explicit decision on the placeholder.
        IColumnWriter<ClickHouseDynamic> sut = new DynamicColumnWriter(WriterFactory());

        Assert.Throws<NotSupportedException>(() => _ = sut.NullPlaceholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullDynamic_RoundTripsAsNullDiscriminator()
    {
        // ClickHouseDynamic.Null is the dedicated NULL sentinel (Discriminator
        // = 255 = NullDiscriminator). The writer must skip it during type
        // collection, write the nullIndex (= number of types) into the
        // indexes column, and contribute nothing to any per-arm bucket.
        // (Note: default(ClickHouseDynamic) has Discriminator = 0 and is NOT
        // a NULL — it would trip the "non-NULL but DeclaredTypeName is null"
        // precondition.)
        var sut = new DynamicColumnWriter(WriterFactory());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[]
        {
            new ClickHouseDynamic(0, "x", "String"),
            ClickHouseDynamic.Null,
            new ClickHouseDynamic(0, "y", "String"),
        });

        var span = buffer.WrittenSpan;
        // VarInt(numberOfTypes=1) = 1 byte
        // String "String": VarInt(6) + 6 bytes = 7
        // Indexes (3 rows, totalIndexValues=2 → fits in byte): 3 bytes
        // String arm: 2 entries → VarInt(1)+'x' + VarInt(1)+'y' = 4 bytes
        Assert.Equal(1 + 7 + 3 + 4, span.Length);

        // Type-name table
        Assert.Equal(0x01, span[0]);  // VarInt 1
        Assert.Equal(0x06, span[1]);  // VarInt 6 ("String")
        Assert.Equal((byte)'S', span[2]);

        // Indexes at indices 8..10: row 0 = 0 (String), row 1 = 1 (NULL = nullIndex = numberOfTypes = 1), row 2 = 0
        Assert.Equal(0x00, span[8]);
        Assert.Equal(0x01, span[9]);
        Assert.Equal(0x00, span[10]);

        // Arm-0 (String) bucket has 2 entries: "x" then "y"
        Assert.Equal(0x01, span[11]);
        Assert.Equal((byte)'x', span[12]);
        Assert.Equal(0x01, span[13]);
        Assert.Equal((byte)'y', span[14]);
    }

    [Fact]
    public void NonGeneric_WriteColumn_NullEntry_MapsToClickHouseDynamicNull()
    {
        // Pin the null → ClickHouseDynamic.Null semantic. Same wire output as
        // passing default(ClickHouseDynamic) through the typed path: the null
        // row produces a nullIndex discriminator, no arm-side bytes.
        IColumnWriter sut = new DynamicColumnWriter(WriterFactory());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new object?[]
        {
            new ClickHouseDynamic(0, "x", "String"),
            null,
        });

        var span = buffer.WrittenSpan;
        // VarInt(1) + VarInt(6)+"String" + 2 indexes (byte each) + arm-0: VarInt(1)+'x'
        Assert.Equal(1 + 7 + 2 + 2, span.Length);

        // Row 0 → index 0; Row 1 (null) → nullIndex = 1
        Assert.Equal(0x00, span[8]);
        Assert.Equal(0x01, span[9]);
    }
}

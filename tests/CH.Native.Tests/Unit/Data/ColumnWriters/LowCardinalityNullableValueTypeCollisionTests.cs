using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix bug: <see cref="LowCardinalityColumnWriter{T}"/> for nullable wrappers
/// detects null via <c>EqualityComparer&lt;T&gt;.Default.Equals(value, default!)</c>
/// after the non-generic <c>WriteColumn(object?[])</c> path collapses real null
/// to <c>default(T)</c>. For value types <c>default(int)=0</c>, so an actual
/// row carrying <c>0</c> in <c>LowCardinality(Nullable(Int32))</c> is silently
/// mapped to the null dictionary slot — written as NULL and round-tripping as
/// NULL. Reference types are unaffected because <c>default(string)=null</c>,
/// so the bug only manifests for value-type T.
/// </summary>
public class LowCardinalityNullableValueTypeCollisionTests
{
    [Fact]
    public void Nullable_Int_NonGeneric_ZeroDoesNotCollideWithNull()
    {
        // LowCardinality(Nullable(Int32)) — slot 0 is reserved for NULL.
        // Row 0 = null, row 1 = 0 (real zero), row 2 = 7.
        // Pre-fix: row 1's `0` collapses to default(int)=0, equality compare
        //          treats it as null, indices[1] = 0 → null leaked.
        // Post-fix: real-zero rows must land on a non-zero dictionary slot.
        IColumnWriter sut = new LowCardinalityColumnWriter<int>(
            new Int32ColumnWriter(), isNullable: true);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new object?[] { null, 0, 7 });

        var span = buffer.WrittenSpan;
        // Layout: flags(8) + dictSize(8) + dict entries + indexCount(8) + indices(UInt8 each)
        var dictSize = BitConverter.ToUInt64(span[8..16]);
        // dict entries: slot 0 = default int (0) for the null slot,
        //               slot 1 = 0 (the real zero row),
        //               slot 2 = 7.
        // Each Int32 entry is 4 bytes raw. Total dict bytes = 4 * dictSize.
        var indexCountOffset = 16 + 4 * (int)dictSize;
        var indexCount = BitConverter.ToUInt64(span[indexCountOffset..(indexCountOffset + 8)]);
        Assert.Equal(3UL, indexCount);

        var indicesOffset = indexCountOffset + 8;
        Assert.Equal(0x00, span[indicesOffset]);     // null → slot 0
        // Row 1 (real 0) MUST NOT be slot 0 — that would leak as null.
        Assert.NotEqual(0x00, span[indicesOffset + 1]);
        // Row 2 (7) is also non-null.
        Assert.NotEqual(0x00, span[indicesOffset + 2]);
        // Row 1 and Row 2 must end up at distinct slots — distinct values.
        Assert.NotEqual(span[indicesOffset + 1], span[indicesOffset + 2]);
    }

    [Fact]
    public void Nullable_Long_NonGeneric_ZeroDoesNotCollideWithNull()
    {
        // Same shape, value-type T = long. default(long) = 0 collides with
        // a real 0L row pre-fix, mapping it to the NULL slot.
        IColumnWriter sut = new LowCardinalityColumnWriter<long>(
            new Int64ColumnWriter(), isNullable: true);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new object?[] { null, 0L, 9999L });

        var span = buffer.WrittenSpan;
        var dictSize = BitConverter.ToUInt64(span[8..16]);
        var indexCountOffset = 16 + 8 * (int)dictSize; // long entries are 8 bytes
        var indexCount = BitConverter.ToUInt64(span[indexCountOffset..(indexCountOffset + 8)]);
        Assert.Equal(3UL, indexCount);

        var indicesOffset = indexCountOffset + 8;
        Assert.Equal(0x00, span[indicesOffset]);
        Assert.NotEqual(0x00, span[indicesOffset + 1]);
        Assert.NotEqual(0x00, span[indicesOffset + 2]);
        Assert.NotEqual(span[indicesOffset + 1], span[indicesOffset + 2]);
    }
}

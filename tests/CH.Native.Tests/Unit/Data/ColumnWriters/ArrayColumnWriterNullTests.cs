using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Mirror of the String/FixedString null tests for Array(T). The original code
/// silently emitted a length-0 row for a null array (<c>values[i]?.Length ?? 0</c>),
/// which is indistinguishable from a real empty array. The fix throws on the
/// bare path and substitutes <see cref="Array.Empty{T}"/> via the Nullable wrapper.
/// </summary>
public class ArrayColumnWriterNullTests
{
    [Fact]
    public void NullPlaceholder_IsEmptyArray()
    {
        Assert.Equal(Array.Empty<int>(), new ArrayColumnWriter<int>(new Int32ColumnWriter()).NullPlaceholder);
    }

    [Fact]
    public void Typed_WriteColumn_NullArrayEntry_Throws()
    {
        var sut = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { new[] { 1, 2 }, null!, new[] { 3 } }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("Array", caught.Message);
    }

    [Fact]
    public void Typed_WriteValue_Null_Throws()
    {
        var sut = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null!); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteColumn_NullArrayEntry_Throws()
    {
        IColumnWriter sut = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { new[] { 1 }, null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new ArrayColumnWriter<int>(new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NullableWrap_WriteColumn_NullSlot_WritesBitmapAndEmptyArrayOffset()
    {
        // Nullable(Array(Int32)): bitmap byte + offsets row (UInt64) + flat
        // element data. A null slot becomes an empty Array.Empty<int>(), which
        // contributes 0 to the flat element count and increments the cumulative
        // offset by 0.
        var sut = new NullableRefColumnWriter<int[]>(new ArrayColumnWriter<int>(new Int32ColumnWriter()));
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[] { new[] { 1, 2 }, null!, new[] { 3 } });

        var span = buffer.WrittenSpan;
        // 3 bitmap bytes + 3 × UInt64 offsets + 3 × Int32 elements (1+2+3 — the
        // null slot contributes 0 elements, but the elements 1,2 from row 0
        // and 3 from row 2 still land in the flat block).
        Assert.Equal(3 + 3 * 8 + 3 * 4, span.Length);

        // Bitmap
        Assert.Equal(0x00, span[0]);
        Assert.Equal(0x01, span[1]);
        Assert.Equal(0x00, span[2]);

        // Cumulative offsets at indices 3..26 (UInt64 little-endian)
        Assert.Equal(2UL, BitConverter.ToUInt64(span[3..11]));   // row 0: 2 elements
        Assert.Equal(2UL, BitConverter.ToUInt64(span[11..19]));  // row 1 (null → empty): still 2
        Assert.Equal(3UL, BitConverter.ToUInt64(span[19..27]));  // row 2: +1 element

        // Flat element block at indices 27..38 (Int32 little-endian)
        Assert.Equal(1, BitConverter.ToInt32(span[27..31]));
        Assert.Equal(2, BitConverter.ToInt32(span[31..35]));
        Assert.Equal(3, BitConverter.ToInt32(span[35..39]));
    }
}

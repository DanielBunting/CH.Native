using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="LowCardinalityColumnWriter{T}"/>.
/// Original behavior coerced null to <c>default(T)</c> via
/// <c>values[i] is T v ? v : default!</c>, which silently landed nulls at the
/// (wrong) "first unique key" dictionary slot. The fix differentiates:
/// <list type="bullet">
///   <item><description><c>LowCardinality(T)</c> (non-nullable) — null throws.</description></item>
///   <item><description><c>LowCardinality(Nullable(T))</c> — null maps to dictionary slot 0 (preserved).</description></item>
/// </list>
/// </summary>
public class LowCardinalityColumnWriterNullTests
{
    private static LowCardinalityColumnWriter<string> NewNonNullable() =>
        new(new StringColumnWriter(), isNullable: false);

    private static LowCardinalityColumnWriter<string> NewNullable() =>
        new(new StringColumnWriter(), isNullable: true);

    [Fact]
    public void NonNullable_Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = NewNonNullable();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { "alpha", null!, "beta" }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("LowCardinality", caught.Message);
    }

    [Fact]
    public void NonNullable_NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = NewNonNullable();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { "alpha", null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonNullable_NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = NewNonNullable();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteColumn_UnsupportedType_Throws()
    {
        IColumnWriter sut = NewNonNullable();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { "alpha", 42 }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("unsupported value type", caught!.Message);
        Assert.Contains("row 1", caught.Message);
    }

    [Fact]
    public void Nullable_Typed_WriteColumn_NullEntry_LandsAtDictionaryIndexZero()
    {
        // Regression guard: LowCardinality(Nullable(String)) MUST keep working
        // — null values map to dictionary slot 0 (the reserved null slot).
        // Verify by checking the index column references slot 0 for the null row.
        var sut = NewNullable();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteColumn(ref writer, new[] { "alpha", null!, "alpha", "beta" });

        var span = buffer.WrittenSpan;
        // Wire layout: flags(8) + dictSize(8) + dictionary entries + indexCount(8) + indices.
        // Dictionary: slot 0 = null sentinel ("" via default), slot 1 = "alpha", slot 2 = "beta".
        // Indices for ["alpha", null, "alpha", "beta"] = [1, 0, 1, 2] (single byte each — UInt8).

        // Dictionary size at offset 8 (after flags)
        Assert.Equal(3UL, BitConverter.ToUInt64(span[8..16]));
        // Index count just before the indices block — find it: dict entries follow at 16.
        // Slot 0: VarInt(0) for empty placeholder = 1 byte.
        // Slot 1: VarInt(5) "alpha" = 6 bytes.
        // Slot 2: VarInt(4) "beta" = 5 bytes.
        // Total dict bytes = 12. Index count at offset 16+12 = 28.
        Assert.Equal(4UL, BitConverter.ToUInt64(span[28..36]));
        // Indices at offset 36..40
        Assert.Equal(0x01, span[36]); // "alpha"
        Assert.Equal(0x00, span[37]); // null → slot 0
        Assert.Equal(0x01, span[38]); // "alpha" (deduped)
        Assert.Equal(0x02, span[39]); // "beta"
    }
}

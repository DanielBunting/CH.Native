using System.Buffers;
using System.Text.Json;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="JsonColumnWriter"/>. The
/// original code silently coerced null to <c>"{}"</c>, indistinguishable from
/// a real empty-object JSON value — same data-corruption pattern the Phase 1
/// String/FixedString and Phase 2 Map/Tuple fixes addressed.
/// </summary>
public class JsonColumnWriterNullTests
{
    [Fact]
    public void Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = new JsonColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new[]
            {
                JsonDocument.Parse("{\"a\":1}"),
                null!,
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("JSON", caught.Message);
    }

    [Fact]
    public void Typed_WriteValue_Null_Throws()
    {
        var sut = new JsonColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null!); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = new JsonColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new object?[] { "{\"a\":1}", null });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new JsonColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void NonGeneric_WriteValue_String_RoundTripsAsRawJson()
    {
        // Sanity: strings still pass through unchanged.
        IColumnWriter sut = new JsonColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        sut.WriteValue(ref writer, "{\"a\":1}");

        var span = buffer.WrittenSpan;
        Assert.Equal(0x07, span[0]); // VarInt length 7
        Assert.Equal((byte)'{', span[1]);
    }
}

using System.Buffers;
using System.Net;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on the three reference-typed scalar writers
/// (<see cref="UuidColumnWriter"/>, <see cref="IPv4ColumnWriter"/>,
/// <see cref="IPv6ColumnWriter"/>). Original behavior was a NullReferenceException
/// on null in the non-generic paths via <c>(Guid)value!</c> / <c>(IPAddress)value!</c>
/// casts — a hard fail with no row context. The fix replaces the NRE with an
/// <see cref="InvalidOperationException"/> that names the column type and row index.
/// </summary>
public class UuidIPv4IPv6ColumnWriterNullTests
{
    // ---------- UuidColumnWriter ----------

    [Fact]
    public void Uuid_NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = new UuidColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { Guid.NewGuid(), null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("UUID", caught.Message);
    }

    [Fact]
    public void Uuid_NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new UuidColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    // ---------- IPv4ColumnWriter ----------

    [Fact]
    public void IPv4_Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = new IPv4ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { IPAddress.Parse("1.2.3.4"), null! }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("IPv4", caught.Message);
    }

    [Fact]
    public void IPv4_NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = new IPv4ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { IPAddress.Parse("1.2.3.4"), null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void IPv4_NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new IPv4ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    // ---------- IPv6ColumnWriter ----------

    [Fact]
    public void IPv6_Typed_WriteColumn_NullEntry_Throws()
    {
        var sut = new IPv6ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new[] { IPAddress.Parse("::1"), null! }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("IPv6", caught.Message);
    }

    [Fact]
    public void IPv6_NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = new IPv6ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteColumn(ref writer, new object?[] { IPAddress.Parse("::1"), null }); }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
    }

    [Fact]
    public void IPv6_NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new IPv6ColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }
}

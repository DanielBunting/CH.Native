using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pins the strict-null contract on <see cref="PointColumnWriter"/>. The
/// original code silently coerced <c>null</c> to <c>Point.Zero</c> in the
/// non-generic paths — coordinate corruption indistinguishable from the
/// origin (0, 0).
/// </summary>
public class PointColumnWriterNullTests
{
    [Fact]
    public void NonGeneric_WriteColumn_NullEntry_Throws()
    {
        IColumnWriter sut = new PointColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try
        {
            sut.WriteColumn(ref writer, new object?[]
            {
                (1.0, 2.0),
                null,
            });
        }
        catch (InvalidOperationException ex) { caught = ex; }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("Point", caught.Message);
    }

    [Fact]
    public void NonGeneric_WriteValue_Null_Throws()
    {
        IColumnWriter sut = new PointColumnWriter();
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        InvalidOperationException? caught = null;
        try { sut.WriteValue(ref writer, null); }
        catch (InvalidOperationException ex) { caught = ex; }
        Assert.NotNull(caught);
    }
}

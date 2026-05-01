using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Pre-fix <see cref="TupleColumnWriter.WriteValue"/> tolerated arity
/// mismatches: a value array shorter than the declared element count
/// silently passed <c>null</c> to the missing inner writers, and an over-long
/// array dropped its trailing elements. Both are silent data corruption.
/// </summary>
public class TupleColumnWriterArityTests
{
    private static TupleColumnWriter NewWriter() =>
        new(new IColumnWriter[] { new Int32ColumnWriter(), new Int32ColumnWriter(), new Int32ColumnWriter() });

    [Fact]
    public void WriteValue_ShortArray_Throws()
    {
        var writer = NewWriter();
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<InvalidOperationException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, new object[] { 1, 2 }); // arity 2 vs declared 3
        });
    }

    [Fact]
    public void WriteValue_LongArray_Throws()
    {
        var writer = NewWriter();
        var bw = new ArrayBufferWriter<byte>();
        Assert.Throws<InvalidOperationException>(() =>
        {
            var pw = new ProtocolWriter(bw);
            writer.WriteValue(ref pw, new object[] { 1, 2, 3, 4 });
        });
    }

    [Fact]
    public void WriteValue_MatchingArity_RoundTrips()
    {
        var writer = NewWriter();
        var bw = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(bw);
        writer.WriteValue(ref pw, new object[] { 1, 2, 3 });
        Assert.Equal(12, bw.WrittenCount); // 3 × Int32
    }
}

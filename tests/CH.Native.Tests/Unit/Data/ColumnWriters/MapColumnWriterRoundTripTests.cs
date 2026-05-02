using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// <see cref="MapColumnWriterNullTests"/> only covers the null-handling axis.
/// This adds end-to-end round-trip coverage so the wire format
/// (offsets, then keys, then values) is locked.
/// </summary>
public class MapColumnWriterRoundTripTests
{
    [Fact]
    public void TypeName_NamespacesKeyAndValue()
    {
        var sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        Assert.Equal("Map(String, Int32)", sut.TypeName);
    }

    [Fact]
    public void RoundTrip_StringToInt32()
    {
        var rows = new[]
        {
            new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
            new Dictionary<string, int>(),
            new Dictionary<string, int> { ["x"] = 42 },
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter())
            .WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, rows.Length);

        for (int i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i].Count, column[i].Count);
            foreach (var (k, v) in rows[i]) Assert.Equal(v, column[i][k]);
        }
    }

    [Fact]
    public void RoundTrip_LargeMap()
    {
        var dict = new Dictionary<string, int>();
        for (int i = 0; i < 1000; i++) dict[$"key_{i}"] = i * 7;
        var rows = new[] { dict };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter())
            .WriteColumn(ref writer, rows);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, rows.Length);

        Assert.Equal(dict.Count, column[0].Count);
        foreach (var (k, v) in dict) Assert.Equal(v, column[0][k]);
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Tests for entries-shape inputs to <see cref="MapColumnWriter{TKey, TValue}"/>:
/// <see cref="ClickHouseMap{TKey, TValue}"/>, <see cref="KeyValuePair{TKey, TValue}"/>[],
/// and various <see cref="IReadOnlyList{T}"/> / <see cref="IEnumerable{T}"/> wrappers.
/// The wire format is identical to the Dictionary input when the entries are
/// duplicate-free, and preserves duplicates losslessly otherwise.
/// </summary>
public class MapColumnWriterEntriesInputTests
{
    private static byte[] WriteViaObjectArray(object?[] rows)
    {
        IColumnWriter sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        sut.WriteColumn(ref writer, rows);
        return buffer.WrittenMemory.ToArray();
    }

    private static byte[] WriteViaDictionaryFastPath(Dictionary<string, int>[] rows)
    {
        var sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        sut.WriteColumn(ref writer, rows);
        return buffer.WrittenMemory.ToArray();
    }

    [Fact]
    public void ClickHouseMap_DuplicateFree_ProducesByteIdenticalWireToDictionary()
    {
        var dict = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 };
        var cmap = new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("b", 2),
        });

        var dictBytes = WriteViaDictionaryFastPath(new[] { dict });
        var entriesBytes = WriteViaObjectArray(new object?[] { cmap });

        Assert.Equal(dictBytes, entriesBytes);
    }

    [Fact]
    public void ClickHouseMap_DuplicateKeys_PreservedOnWire()
    {
        var cmap = new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
            new KeyValuePair<string, int>("b", 3),
        });

        var bytes = WriteViaObjectArray(new object?[] { cmap });

        // Round-trip via the entries reader to confirm all 3 entries survived.
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(3, col[0].Count);
        Assert.True(col[0].HasDuplicateKeys);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), col[0][0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), col[0][1]);
        Assert.Equal(new KeyValuePair<string, int>("b", 3), col[0][2]);
    }

    [Fact]
    public void KeyValuePairArray_DuplicateKeys_PreservedOnWire()
    {
        var arr = new[]
        {
            new KeyValuePair<string, int>("x", 10),
            new KeyValuePair<string, int>("x", 20),
        };

        var bytes = WriteViaObjectArray(new object?[] { arr });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(2, col[0].Count);
        Assert.Equal(new KeyValuePair<string, int>("x", 10), col[0][0]);
        Assert.Equal(new KeyValuePair<string, int>("x", 20), col[0][1]);
    }

    [Fact]
    public void IReadOnlyListOfKvp_DuplicateKeys_PreservedOnWire()
    {
        IReadOnlyList<KeyValuePair<string, int>> list = new List<KeyValuePair<string, int>>
        {
            new("k", 1),
            new("k", 2),
            new("k", 3),
        };

        var bytes = WriteViaObjectArray(new object?[] { list });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(3, col[0].Count);
        Assert.Equal(1, col[0][0].Value);
        Assert.Equal(2, col[0][1].Value);
        Assert.Equal(3, col[0][2].Value);
    }

    [Fact]
    public void EnumerableOfKvp_PreservedOnWire()
    {
        static IEnumerable<KeyValuePair<string, int>> Source()
        {
            yield return new KeyValuePair<string, int>("a", 1);
            yield return new KeyValuePair<string, int>("b", 2);
        }

        var bytes = WriteViaObjectArray(new object?[] { Source() });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(2, col[0].Count);
        Assert.Equal(1, col[0]["a"]);
        Assert.Equal(2, col[0]["b"]);
    }

    [Fact]
    public void MixedShapeRows_AllProduceCorrectWire()
    {
        var dict = new Dictionary<string, int> { ["a"] = 1 };
        var cmap = new ClickHouseMap<string, int>(new[] { new KeyValuePair<string, int>("b", 2) });
        var arr = new[] { new KeyValuePair<string, int>("c", 3) };

        var bytes = WriteViaObjectArray(new object?[] { dict, cmap, arr });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 3);

        Assert.Equal(1, col[0]["a"]);
        Assert.Equal(2, col[1]["b"]);
        Assert.Equal(3, col[2]["c"]);
    }

    [Fact]
    public void IDictionary_CompatPath_StillWorks()
    {
        // Custom IDictionary (non-generic, hashtable-style) — must continue to work
        // for F# Map<,> / third-party dictionary types via the existing compat path.
        var customDict = new System.Collections.Hashtable
        {
            ["a"] = 1,
            ["b"] = 2,
        };

        var bytes = WriteViaObjectArray(new object?[] { customDict });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(2, col[0].Count);
        Assert.Contains(col[0], kv => kv.Key == "a" && kv.Value == 1);
        Assert.Contains(col[0], kv => kv.Key == "b" && kv.Value == 2);
    }

    [Fact]
    public void UnsupportedShape_ThrowsHelpfulError()
    {
        InvalidOperationException? captured = null;
        try
        {
            var sut = (IColumnWriter)new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new ProtocolWriter(buffer);
            sut.WriteColumn(ref writer, new object?[] { 42 });
        }
        catch (InvalidOperationException ex)
        {
            captured = ex;
        }

        Assert.NotNull(captured);
        Assert.Contains("MapColumnWriter", captured!.Message);
    }
}

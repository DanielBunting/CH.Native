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

    [Fact]
    public void IListOnlyOfKvp_DefensiveCopyPath_RoundTrips()
    {
        // List<KVP> matches IReadOnlyList<KVP> first and bypasses the IList<KVP>
        // branch. A custom collection that implements only IList<KVP> (no
        // IReadOnlyList<KVP>) forces the defensive copy at MapColumnWriter:188-198.
        var custom = new IListOnlyKvpCollection
        {
            new("p", 1),
            new("q", 2),
            new("q", 3),
        };

        var bytes = WriteViaObjectArray(new object?[] { custom });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 1);

        Assert.Equal(3, col[0].Count);
        Assert.Equal(new KeyValuePair<string, int>("p", 1), col[0][0]);
        Assert.Equal(new KeyValuePair<string, int>("q", 2), col[0][1]);
        Assert.Equal(new KeyValuePair<string, int>("q", 3), col[0][2]);
    }

    [Fact]
    public void LegacyIDictionary_InterleavedWithDictAndEntries_CompatPathRoundTrips()
    {
        // Forces the compat path (MapColumnWriter:284-326): when any row is a
        // non-generic IDictionary, all rows are written via the per-value writer
        // — including the interleaved Dictionary and entries branches at :292-296
        // and :297-301 which a single-Hashtable test never exercises.
        var hash = new System.Collections.Hashtable
        {
            ["h1"] = 100,
            ["h2"] = 200,
        };
        var dict = new Dictionary<string, int> { ["d"] = 1 };
        var cmap = new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("c", 7),
            new KeyValuePair<string, int>("c", 8),
        });
        var arr = new[] { new KeyValuePair<string, int>("a", 42) };

        var bytes = WriteViaObjectArray(new object?[] { hash, dict, cmap, arr });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var col = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 4);

        // Hashtable enumeration order isn't guaranteed; just assert content.
        Assert.Equal(2, col[0].Count);
        Assert.Contains(col[0], kv => kv.Key == "h1" && kv.Value == 100);
        Assert.Contains(col[0], kv => kv.Key == "h2" && kv.Value == 200);

        Assert.Equal(1, col[1].Count);
        Assert.Equal(new KeyValuePair<string, int>("d", 1), col[1][0]);

        Assert.Equal(2, col[2].Count);
        Assert.True(col[2].HasDuplicateKeys);
        Assert.Equal(new KeyValuePair<string, int>("c", 7), col[2][0]);
        Assert.Equal(new KeyValuePair<string, int>("c", 8), col[2][1]);

        Assert.Equal(1, col[3].Count);
        Assert.Equal(new KeyValuePair<string, int>("a", 42), col[3][0]);
    }

    private static byte[] WriteValueViaInterface(object value)
    {
        IColumnWriter sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        sut.WriteValue(ref writer, value);
        return buffer.WrittenMemory.ToArray();
    }

    private static ClickHouseMap<string, int> ReadSingleValue(byte[] bytes)
    {
        var reader = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader());
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        return reader.ReadValue(ref pr);
    }

    [Fact]
    public void WriteValue_PerRow_DispatchesAllSupportedShapes()
    {
        // IColumnWriter.WriteValue is the per-row entry point used by callers that
        // don't go through the bulk WriteColumn path. Exercise each switch arm
        // (Dictionary, ClickHouseMap, KVP[], IReadOnlyList, IEnumerable) so a
        // refactor of the dispatch doesn't silently drop a supported shape.
        var fromDict = ReadSingleValue(WriteValueViaInterface(new Dictionary<string, int> { ["a"] = 1 }));
        Assert.Equal(1, fromDict.Count);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), fromDict[0]);

        var fromCmap = ReadSingleValue(WriteValueViaInterface(new ClickHouseMap<string, int>(new[]
        {
            new KeyValuePair<string, int>("a", 1),
            new KeyValuePair<string, int>("a", 2),
        })));
        Assert.Equal(2, fromCmap.Count);
        Assert.True(fromCmap.HasDuplicateKeys);

        var fromArr = ReadSingleValue(WriteValueViaInterface(new[] { new KeyValuePair<string, int>("k", 7) }));
        Assert.Equal(1, fromArr.Count);
        Assert.Equal(new KeyValuePair<string, int>("k", 7), fromArr[0]);

        IReadOnlyList<KeyValuePair<string, int>> rolist = new List<KeyValuePair<string, int>>
        {
            new("r", 1), new("r", 2),
        };
        var fromRoList = ReadSingleValue(WriteValueViaInterface(rolist));
        Assert.Equal(2, fromRoList.Count);

        var fromEnumerable = ReadSingleValue(WriteValueViaInterface(KvpEnumerableSource()));
        Assert.Equal(1, fromEnumerable.Count);
        Assert.Equal(new KeyValuePair<string, int>("e", 9), fromEnumerable[0]);
    }

    private static IEnumerable<KeyValuePair<string, int>> KvpEnumerableSource()
    {
        yield return new KeyValuePair<string, int>("e", 9);
    }

    [Fact]
    public void WriteValue_PerRow_Null_Throws()
    {
        IColumnWriter sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();

        InvalidOperationException? captured = null;
        try
        {
            var writer = new ProtocolWriter(buffer);
            sut.WriteValue(ref writer, null);
        }
        catch (InvalidOperationException ex)
        {
            captured = ex;
        }

        Assert.NotNull(captured);
        Assert.Contains("MapColumnWriter", captured!.Message);
    }

    [Fact]
    public void WriteValue_PerRow_UnsupportedShape_Throws()
    {
        IColumnWriter sut = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        var buffer = new ArrayBufferWriter<byte>();

        InvalidOperationException? captured = null;
        try
        {
            var writer = new ProtocolWriter(buffer);
            sut.WriteValue(ref writer, 42);
        }
        catch (InvalidOperationException ex)
        {
            captured = ex;
        }

        Assert.NotNull(captured);
        Assert.Contains("MapColumnWriter", captured!.Message);
        Assert.Contains("unsupported", captured.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed class IListOnlyKvpCollection : IList<KeyValuePair<string, int>>
    {
        // Deliberately does NOT implement IReadOnlyList<KVP> so that pattern
        // matching falls through to the IList<KVP> branch.
        private readonly List<KeyValuePair<string, int>> _inner = new();

        public KeyValuePair<string, int> this[int index] { get => _inner[index]; set => _inner[index] = value; }
        public int Count => _inner.Count;
        public bool IsReadOnly => false;
        public void Add(KeyValuePair<string, int> item) => _inner.Add(item);
        public void Clear() => _inner.Clear();
        public bool Contains(KeyValuePair<string, int> item) => _inner.Contains(item);
        public void CopyTo(KeyValuePair<string, int>[] array, int arrayIndex) => _inner.CopyTo(array, arrayIndex);
        public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => _inner.GetEnumerator();
        public int IndexOf(KeyValuePair<string, int> item) => _inner.IndexOf(item);
        public void Insert(int index, KeyValuePair<string, int> item) => _inner.Insert(index, item);
        public bool Remove(KeyValuePair<string, int> item) => _inner.Remove(item);
        public void RemoveAt(int index) => _inner.RemoveAt(index);
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _inner.GetEnumerator();
    }
}

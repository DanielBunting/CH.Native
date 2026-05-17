using System.Buffers;
using BenchmarkDotNet.Attributes;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Mapping;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Unit-level benchmarks for the Map(K, V) read/write path, including the new
/// lossless <see cref="ClickHouseMap{TKey, TValue}"/> shape introduced alongside
/// <see cref="MapEntriesColumnReader{TKey, TValue}"/>.
///
/// Goals:
/// 1. Lock in the Dictionary bulk-insert fast path (regression guard) — the new
///    multi-shape dispatch must not introduce per-row allocation on the common
///    typed-Dictionary input.
/// 2. Measure the ClickHouseMap / KeyValuePair[] entries write paths.
/// 3. Compare per-row read cost of <see cref="MapColumnReader{TKey, TValue}"/>
///    (Dictionary) vs <see cref="MapEntriesColumnReader{TKey, TValue}"/> (ClickHouseMap).
/// 4. Confirm <see cref="MapShapeInspector"/> caching collapses the per-call
///    reflection cost to a single dictionary lookup.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class MapColumnBenchmarks
{
    [Params(100, 1000, 10000)]
    public int RowCount { get; set; }

    private const int EntriesPerRow = 8;

    private Dictionary<string, int>[] _dictRows = null!;
    private ClickHouseMap<string, int>[] _entriesRows = null!;
    private KeyValuePair<string, int>[][] _kvpArrayRows = null!;
    private byte[] _readerSourceBytes = null!;
    private MapColumnWriter<string, int> _writer = null!;
    private MapColumnReader<string, int> _dictReader = null!;
    private MapEntriesColumnReader<string, int> _entriesReader = null!;

    [GlobalSetup]
    public void Setup()
    {
        _writer = new MapColumnWriter<string, int>(new StringColumnWriter(), new Int32ColumnWriter());
        _dictReader = new MapColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader());
        _entriesReader = new MapEntriesColumnReader<string, int>(new StringColumnReader(), new Int32ColumnReader());

        var rng = new Random(42);

        _dictRows = new Dictionary<string, int>[RowCount];
        _entriesRows = new ClickHouseMap<string, int>[RowCount];
        _kvpArrayRows = new KeyValuePair<string, int>[RowCount][];

        for (int i = 0; i < RowCount; i++)
        {
            var entries = new KeyValuePair<string, int>[EntriesPerRow];
            var dict = new Dictionary<string, int>(EntriesPerRow);
            for (int j = 0; j < EntriesPerRow; j++)
            {
                var key = $"k{j}_{rng.Next(1000)}";
                var value = rng.Next();
                entries[j] = new KeyValuePair<string, int>(key, value);
                dict[key] = value;
            }
            _dictRows[i] = dict;
            _kvpArrayRows[i] = entries;
            _entriesRows[i] = new ClickHouseMap<string, int>(entries);
        }

        // Pre-serialise one column's worth of Map data for the read benchmarks. We
        // use the Dictionary writer path because both readers consume the same wire
        // format (entries reader is lossless; Dictionary reader collapses duplicates,
        // which here are absent).
        var buffer = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(buffer);
        _writer.WriteColumn(ref w, _dictRows);
        _readerSourceBytes = buffer.WrittenMemory.ToArray();
    }

    /// <summary>
    /// Baseline: typed Dictionary bulk write. This is the path the new
    /// multi-shape dispatch must not regress.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int Write_Dictionary_Typed()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        _writer.WriteColumn(ref writer, _dictRows);
        return buffer.WrittenCount;
    }

    /// <summary>
    /// Dictionary via the IColumnWriter (non-generic) dispatch — exercises the
    /// new switch table on the common Dictionary input. Should hit the restored
    /// pure-Dictionary fast path with no per-row buffer allocation.
    /// </summary>
    [Benchmark]
    public int Write_Dictionary_NonGenericDispatch()
    {
        var boxed = new object?[_dictRows.Length];
        for (int i = 0; i < _dictRows.Length; i++) boxed[i] = _dictRows[i];

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        ((IColumnWriter)_writer).WriteColumn(ref writer, boxed);
        return buffer.WrittenCount;
    }

    /// <summary>
    /// New entries-shape input: ClickHouseMap. Goes through the entries[] slot
    /// (zero-copy reference store) and the mixed-no-legacy flatten path.
    /// </summary>
    [Benchmark]
    public int Write_ClickHouseMap_NonGenericDispatch()
    {
        var boxed = new object?[_entriesRows.Length];
        for (int i = 0; i < _entriesRows.Length; i++) boxed[i] = _entriesRows[i];

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        ((IColumnWriter)_writer).WriteColumn(ref writer, boxed);
        return buffer.WrittenCount;
    }

    /// <summary>
    /// New entries-shape input: KeyValuePair[]. Zero-copy reference store into
    /// entries[] then bulk flatten.
    /// </summary>
    [Benchmark]
    public int Write_KeyValuePairArray_NonGenericDispatch()
    {
        var boxed = new object?[_kvpArrayRows.Length];
        for (int i = 0; i < _kvpArrayRows.Length; i++) boxed[i] = _kvpArrayRows[i];

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        ((IColumnWriter)_writer).WriteColumn(ref writer, boxed);
        return buffer.WrittenCount;
    }

    /// <summary>
    /// Baseline read: materialise the column as Dictionary (legacy reader).
    /// </summary>
    [Benchmark]
    public int Read_AsDictionary()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_readerSourceBytes));
        using var column = _dictReader.ReadTypedColumn(ref reader, RowCount);
        return column.Count;
    }

    /// <summary>
    /// Read via the new entries reader, materialising as ClickHouseMap. Per-row
    /// allocation is one KeyValuePair[] + one ClickHouseMap wrapper, comparable
    /// to one Dictionary per row in the baseline.
    /// </summary>
    [Benchmark]
    public int Read_AsClickHouseMap()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_readerSourceBytes));
        using var column = _entriesReader.ReadTypedColumn(ref reader, RowCount);
        return column.Count;
    }
}

/// <summary>
/// Benchmarks for <see cref="MapShapeInspector"/> — the per-typed-query reflection
/// path. The cache is what makes Layer 1 selection cheap enough to run on every
/// <c>QueryAsync&lt;T&gt;</c> / <c>ExecuteScalarAsync&lt;T&gt;</c> call.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class MapShapeInspectorBenchmarks
{
    private class PocoWithClickHouseMap
    {
        public int Id { get; set; }
        public ClickHouseMap<string, int> Map1 { get; set; } = null!;
        public Dictionary<string, int> Map2 { get; set; } = null!;
        public string Name { get; set; } = string.Empty;
    }

    private class PocoWithoutMap
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime Created { get; set; }
    }

    [GlobalSetup]
    public void Setup()
    {
        // Warm the cache so the steady-state benchmarks below measure the cached path.
        _ = MapShapeInspector.Inspect(typeof(PocoWithClickHouseMap));
        _ = MapShapeInspector.Inspect(typeof(PocoWithoutMap));
        _ = MapShapeInspector.InspectScalar(typeof(ClickHouseMap<string, int>));
    }

    /// <summary>
    /// Cached lookup for a POCO with Map-shape properties — should be a single
    /// ConcurrentDictionary read.
    /// </summary>
    [Benchmark]
    public int Inspect_PocoWithMap_Cached()
    {
        var hints = MapShapeInspector.Inspect(typeof(PocoWithClickHouseMap));
        return hints.Count;
    }

    /// <summary>
    /// The "T has nothing of interest" hot path. Must return the shared Empty
    /// sentinel so callers can short-circuit via ReferenceEquals — no allocation.
    /// </summary>
    [Benchmark]
    public bool Inspect_PocoWithoutMap_Cached_HitsEmptySentinel()
    {
        var hints = MapShapeInspector.Inspect(typeof(PocoWithoutMap));
        return ReferenceEquals(hints, MapShapeInspector.Empty);
    }

    /// <summary>
    /// Scalar T classification on the cached path. Returns int so the benchmark
    /// stays public despite the internal MapShape enum.
    /// </summary>
    [Benchmark]
    public int InspectScalar_ClickHouseMap_Cached()
    {
        return (int)MapShapeInspector.InspectScalar(typeof(ClickHouseMap<string, int>));
    }
}

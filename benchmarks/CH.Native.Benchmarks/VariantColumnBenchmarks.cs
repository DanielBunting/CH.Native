using System.Buffers;
using BenchmarkDotNet.Attributes;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// In-process micro-benchmarks for Variant and Dynamic column readers/writers.
/// No ClickHouse server is required — these measure the CH.Native serialization fast path.
/// End-to-end comparison against ClickHouse.Driver lives in
/// <see cref="Benchmarks.VariantComparisonBenchmarks"/>.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 7)]
public class VariantColumnBenchmarks
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);
    private static readonly ColumnWriterFactory WriterFactory = new(ColumnWriterRegistry.Default);

    [Params(1_000, 10_000, 100_000)]
    public int RowCount { get; set; }

    private ClickHouseVariant[] _mixedRows = null!;
    private byte[] _encodedMixed = null!;
    private VariantColumnReader _reader = null!;
    private VariantColumnWriter _writer = null!;

    [GlobalSetup]
    public void Setup()
    {
        _reader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");
        _writer = (VariantColumnWriter)WriterFactory.CreateWriter("Variant(Int64, String)");

        var rng = new Random(42);
        _mixedRows = new ClickHouseVariant[RowCount];
        for (int i = 0; i < RowCount; i++)
        {
            var roll = rng.Next(3);
            _mixedRows[i] = roll switch
            {
                0 => ClickHouseVariant.Null,
                1 => new ClickHouseVariant(0, (long)rng.Next()),
                _ => new ClickHouseVariant(1, $"s{rng.Next():X}"),
            };
        }

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        _writer.WriteColumn(ref pw, _mixedRows);
        _encodedMixed = buffer.WrittenSpan.ToArray();
    }

    [Benchmark(Description = "Variant write (mixed arms + NULL)")]
    public int Write()
    {
        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        _writer.WriteColumn(ref pw, _mixedRows);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Variant read (mixed arms + NULL)")]
    public int Read()
    {
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(_encodedMixed));
        using var col = _reader.ReadTypedColumn(ref pr, RowCount);
        return col.Count;
    }

    [Benchmark(Description = "Variant round-trip (write then read)")]
    public int RoundTrip()
    {
        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        _writer.WriteColumn(ref pw, _mixedRows);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = _reader.ReadTypedColumn(ref pr, RowCount);
        return col.Count;
    }
}

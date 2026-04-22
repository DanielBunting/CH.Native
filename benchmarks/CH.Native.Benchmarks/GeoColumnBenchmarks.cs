using System.Buffers;
using BenchmarkDotNet.Attributes;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Unit-level benchmarks for geo column readers, writers, and skippers.
/// No Docker required — works directly against the protocol layer.
///
/// Goals:
/// 1. Measure the delegation cost of Point/Ring/etc. vs composing raw Tuple/Array readers directly.
/// 2. Set a baseline for nested-array throughput at realistic row counts.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class GeoColumnBenchmarks
{
    private const int PointRowCount = 10_000;
    private const int RingRowCount = 1_000;
    private const int RingAveragePoints = 16;
    private const int MultiPolygonRowCount = 100;

    private byte[] _pointColumnBytes = null!;
    private byte[] _ringColumnBytes = null!;
    private byte[] _multiPolygonColumnBytes = null!;

    private Point[] _pointValues = null!;
    private Point[][] _ringValues = null!;
    private Point[][][][] _multiPolygonValues = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);

        _pointValues = new Point[PointRowCount];
        for (int i = 0; i < PointRowCount; i++)
            _pointValues[i] = new Point(rng.NextDouble() * 180 - 90, rng.NextDouble() * 180 - 90);

        _ringValues = new Point[RingRowCount][];
        for (int i = 0; i < RingRowCount; i++)
        {
            var ring = new Point[RingAveragePoints];
            for (int j = 0; j < RingAveragePoints; j++)
                ring[j] = new Point(rng.NextDouble(), rng.NextDouble());
            _ringValues[i] = ring;
        }

        _multiPolygonValues = new Point[MultiPolygonRowCount][][][];
        for (int i = 0; i < MultiPolygonRowCount; i++)
        {
            // Each MultiPolygon has 2 polygons; each polygon has 1 outer ring of 8 points
            var poly1 = new Point[][] { GenerateRing(rng, 8) };
            var poly2 = new Point[][] { GenerateRing(rng, 8) };
            _multiPolygonValues[i] = new Point[][][] { poly1, poly2 };
        }

        _pointColumnBytes = SerializeColumn(new PointColumnWriter(), _pointValues);
        _ringColumnBytes = SerializeColumn(new RingColumnWriter(), _ringValues);
        _multiPolygonColumnBytes = SerializeColumn(new MultiPolygonColumnWriter(), _multiPolygonValues);
    }

    private static Point[] GenerateRing(Random rng, int count)
    {
        var ring = new Point[count];
        for (int i = 0; i < count; i++)
            ring[i] = new Point(rng.NextDouble(), rng.NextDouble());
        return ring;
    }

    private static byte[] SerializeColumn<T>(IColumnWriter<T> writer, T[] values)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WriteColumn(ref pw, values);
        return buffer.WrittenSpan.ToArray();
    }

    // --- Point ---

    [Benchmark(Description = "Read 10K Points")]
    public int Reader_PointColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_pointColumnBytes));
        using var column = new PointColumnReader().ReadTypedColumn(ref reader, PointRowCount);
        return column.Count;
    }

    [Benchmark(Description = "Write 10K Points")]
    public int Writer_PointColumn()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new PointColumnWriter().WriteColumn(ref writer, _pointValues);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Skip 10K Points")]
    public bool Skipper_PointColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_pointColumnBytes));
        return new PointColumnSkipper().TrySkipColumn(ref reader, PointRowCount);
    }

    // --- Ring (Array(Point)) ---

    [Benchmark(Description = "Read 1K Rings (16 pts each)")]
    public int Reader_RingColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_ringColumnBytes));
        using var column = new RingColumnReader().ReadTypedColumn(ref reader, RingRowCount);
        return column.Count;
    }

    [Benchmark(Description = "Write 1K Rings (16 pts each)")]
    public int Writer_RingColumn()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new RingColumnWriter().WriteColumn(ref writer, _ringValues);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Skip 1K Rings")]
    public bool Skipper_RingColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_ringColumnBytes));
        return new RingColumnSkipper().TrySkipColumn(ref reader, RingRowCount);
    }

    // --- MultiPolygon ---

    [Benchmark(Description = "Read 100 MultiPolygons")]
    public int Reader_MultiPolygonColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiPolygonColumnBytes));
        using var column = new MultiPolygonColumnReader().ReadTypedColumn(ref reader, MultiPolygonRowCount);
        return column.Count;
    }

    [Benchmark(Description = "Write 100 MultiPolygons")]
    public int Writer_MultiPolygonColumn()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new MultiPolygonColumnWriter().WriteColumn(ref writer, _multiPolygonValues);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Skip 100 MultiPolygons")]
    public bool Skipper_MultiPolygonColumn()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiPolygonColumnBytes));
        return new MultiPolygonColumnSkipper().TrySkipColumn(ref reader, MultiPolygonRowCount);
    }

    // --- Delegation-overhead comparison: Point via alias vs raw Tuple(Float64, Float64) ---
    //
    // The alias PointColumnReader wraps two Float64ColumnReader instances and zips the columnar
    // output into Point[]. The "raw tuple" baseline reads the same wire bytes through the
    // generic TupleColumnReader (which returns boxed System.Tuple<double,double>). A large gap
    // in either direction flags a hot-path surprise.

    [Benchmark(Description = "Read 10K Points via raw Tuple reader")]
    public int Reader_RawTupleBaseline()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_pointColumnBytes));
        var tupleReader = new TupleColumnReader(
            new IColumnReader[] { new Float64ColumnReader(), new Float64ColumnReader() },
            fieldNames: null);
        using var column = tupleReader.ReadTypedColumn(ref reader, PointRowCount);
        return column.Count;
    }
}

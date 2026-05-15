using System.Buffers;
using BenchmarkDotNet.Attributes;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Conversion;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Unit-level benchmarks for the multi-dim array support. No Docker required — exercises
/// the boundary converters and the Array(Array(Int32)) column reader/writer pipeline
/// directly against in-memory protocol buffers.
///
/// Goals:
/// 1. Quantify the typed-vs-reflection cost gap inside RectangularArrayConverter /
///    JaggedToRectangularConverter so future changes can't silently regress it.
/// 2. Measure the *additional* boundary cost when bulk-inserting rectangular data
///    relative to the pre-existing jagged baseline at a realistic workload.
/// 3. Measure read-side rect conversion as a fraction of total read cost.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class MultiDimArrayBenchmarks
{
    // Workload shape:
    //   Column type: Array(Array(Int32))
    //   Rows        = number of outer column rows
    //   Inner       = number of nested arrays per row
    //   Leaf        = number of ints per nested array
    //   Total cells per call ≈ Rows × Inner × Leaf = 16_000 (same order as GeoColumnBenchmarks' Ring suite)
    private const int Rows = 100;
    private const int Inner = 10;
    private const int Leaf = 16;

    private int[,] _bigRect = null!;
    private int[][] _bigJagged = null!;
    private int[,,] _bigRect3D = null!;
    private List<int[,]> _rectRows = null!;
    private int[][][] _jaggedRows = null!;
    private byte[] _arrayArrayIntBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);

        // 2D / 3D rectangles used by the pure converter benchmarks.
        _bigRect = new int[Inner * Rows, Leaf]; // 1000 × 16 — one big rect.
        for (int i = 0; i < _bigRect.GetLength(0); i++)
            for (int j = 0; j < _bigRect.GetLength(1); j++)
                _bigRect[i, j] = rng.Next();

        _bigJagged = new int[_bigRect.GetLength(0)][];
        for (int i = 0; i < _bigJagged.Length; i++)
        {
            var row = new int[Leaf];
            for (int j = 0; j < Leaf; j++)
                row[j] = rng.Next();
            _bigJagged[i] = row;
        }

        _bigRect3D = new int[Rows, Inner, Leaf]; // 100 × 10 × 16.
        for (int i = 0; i < Rows; i++)
            for (int j = 0; j < Inner; j++)
                for (int k = 0; k < Leaf; k++)
                    _bigRect3D[i, j, k] = rng.Next();

        // Per-row payloads for the column-pipeline benchmarks: each column row is one
        // rectangular int[Inner, Leaf]; the jagged variant is its converted equivalent.
        _rectRows = new List<int[,]>(Rows);
        _jaggedRows = new int[Rows][][];
        for (int r = 0; r < Rows; r++)
        {
            var rect = new int[Inner, Leaf];
            var jagged = new int[Inner][];
            for (int i = 0; i < Inner; i++)
            {
                var leaf = new int[Leaf];
                for (int j = 0; j < Leaf; j++)
                {
                    var v = rng.Next();
                    rect[i, j] = v;
                    leaf[j] = v;
                }
                jagged[i] = leaf;
            }
            _rectRows.Add(rect);
            _jaggedRows[r] = jagged;
        }

        // Pre-serialize the Array(Array(Int32)) column once so the read benchmarks have
        // realistic input bytes without re-running the writer per iteration.
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int[]>(new ArrayColumnWriter<int>(new Int32ColumnWriter()))
            .WriteColumn(ref pw, _jaggedRows);
        _arrayArrayIntBytes = buffer.WrittenSpan.ToArray();
    }

    // --- Boundary converter throughput ---

    [Benchmark(Description = "Rect→Jagged 2D (typed fast path, 1000×16)")]
    public int Convert_2D_TypedFastPath()
    {
        var jagged = RectangularArrayConverter.To2DJagged(_bigRect);
        return jagged.Length;
    }

    [Benchmark(Description = "Rect→Jagged 2D (reflection, 1000×16)")]
    public int Convert_2D_Reflection()
    {
        var jagged = RectangularArrayConverter.ToJagged(_bigRect);
        return jagged.Length;
    }

    [Benchmark(Description = "Rect→Jagged 3D (typed fast path, 100×10×16)")]
    public int Convert_3D_TypedFastPath()
    {
        var jagged = RectangularArrayConverter.To3DJagged(_bigRect3D);
        return jagged.Length;
    }

    [Benchmark(Description = "Jagged→Rect 2D (typed fast path, 1000×16)")]
    public int JaggedToRect_2D_TypedFastPath()
    {
        var rect = JaggedToRectangularConverter.ToRectangular2D(_bigJagged);
        return rect.GetLength(0);
    }

    [Benchmark(Description = "Jagged→Rect 2D (reflection, 1000×16)")]
    public int JaggedToRect_2D_Reflection()
    {
        var rect = JaggedToRectangularConverter.ToRectangular(_bigJagged, typeof(int[,]));
        return rect.GetLength(0);
    }

    // --- Column writer pipeline (Array(Array(Int32)), 100 rows × 10 × 16) ---
    //
    // Baseline (FromJagged) is the pre-existing path. FromRectangular runs the per-row
    // converter inside the benchmark — the delta is the added bulk-insert cost.

    [Benchmark(Description = "Write Array(Array(Int32)) from jagged rows")]
    public int Write_ArrayArrayInt_FromJagged()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int[]>(new ArrayColumnWriter<int>(new Int32ColumnWriter()))
            .WriteColumn(ref writer, _jaggedRows);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Write Array(Array(Int32)) from rect rows (per-row convert)")]
    public int Write_ArrayArrayInt_FromRectangular()
    {
        var jagged = new int[_rectRows.Count][][];
        for (int i = 0; i < _rectRows.Count; i++)
            jagged[i] = RectangularArrayConverter.To2DJagged(_rectRows[i]);

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new ArrayColumnWriter<int[]>(new ArrayColumnWriter<int>(new Int32ColumnWriter()))
            .WriteColumn(ref writer, jagged);
        return buffer.WrittenCount;
    }

    // --- Column reader pipeline ---

    [Benchmark(Description = "Read Array(Array(Int32)) as jagged")]
    public int Read_ArrayArrayInt_AsJagged()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_arrayArrayIntBytes));
        using var column = new ArrayColumnReader<int[]>(
            new ArrayColumnReader<int>(new Int32ColumnReader()))
            .ReadTypedColumn(ref reader, Rows);
        return column.Count;
    }

    [Benchmark(Description = "Read Array(Array(Int32)) and rectangularize per row")]
    public int Read_ArrayArrayInt_AsRectangular()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_arrayArrayIntBytes));
        using var column = new ArrayColumnReader<int[]>(
            new ArrayColumnReader<int>(new Int32ColumnReader()))
            .ReadTypedColumn(ref reader, Rows);

        int total = 0;
        for (int i = 0; i < column.Count; i++)
        {
            var rect = JaggedToRectangularConverter.ToRectangular2D(column[i]);
            total += rect.GetLength(0);
        }
        return total;
    }
}

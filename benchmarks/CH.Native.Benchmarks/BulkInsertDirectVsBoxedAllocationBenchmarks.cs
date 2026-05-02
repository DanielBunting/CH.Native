using System.Buffers;
using System.Linq.Expressions;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using CH.Native.BulkInsert;
using CH.Native.Data;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Pins the absolute "boxed allocates strictly more than direct" claim at
/// sustained workloads. Tier-3 system test
/// <c>PreferDirectStreamingFalseFallbackTests</c> intentionally relaxes its
/// invariant to <c>fallbackBytes &gt;= directBytes * 0.5</c> because at
/// 1 000 rows the per-batch overhead dominates and the gap is in the noise
/// — see <c>.tmp/docs/AddedTests-AdditionalFindings.md</c> finding #10. This
/// benchmark amortises that per-batch cost across 50 K+ rows so the gap
/// surfaces stably and can be tracked against a checked-in baseline.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class BulkInsertDirectVsBoxedAllocationBenchmarks
{
    private SimpleRow[] _rows = null!;
    private Func<SimpleRow, object?>[] _getters = null!;
    private string[] _columnNames = null!;
    private string[] _columnTypes = null!;
    private ColumnWriterRegistry _writerRegistry = null!;
    private IColumnExtractor<SimpleRow>[] _extractors = null!;

    [Params(50_000, 100_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _writerRegistry = ColumnWriterRegistry.Default;

        _rows = Enumerable.Range(0, RowCount)
            .Select(i => new SimpleRow { Id = i, Name = $"Item_{i}" })
            .ToArray();

        var props = typeof(SimpleRow).GetProperties();
        _getters = props.Select(p => CreateGetter<SimpleRow>(p)).ToArray();
        _columnNames = props.Select(p => p.Name).ToArray();
        _columnTypes = new[] { "Int32", "String" };

        _extractors = new IColumnExtractor<SimpleRow>[]
        {
            ColumnExtractorFactory.Create<SimpleRow>(props.First(p => p.Name == "Id"), "Id", "Int32"),
            ColumnExtractorFactory.Create<SimpleRow>(props.First(p => p.Name == "Name"), "Name", "String"),
        };
    }

    private static Func<T, object?> CreateGetter<T>(PropertyInfo property)
    {
        var parameter = Expression.Parameter(typeof(T), "obj");
        var propertyAccess = Expression.Property(parameter, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        var lambda = Expression.Lambda<Func<T, object?>>(convert, parameter);
        return lambda.Compile();
    }

    /// <summary>
    /// Direct path (PreferDirectStreaming=true): per-row values are written
    /// straight into the protocol buffer via typed extractors with no
    /// intermediate <c>object?[]</c> column arrays.
    /// </summary>
    [Benchmark(Baseline = true)]
    public int DirectPath()
    {
        var buffer = new ArrayBufferWriter<byte>(1024 * 1024);
        var writer = new ProtocolWriter(buffer);

        WriteDataBlockDirect(ref writer, _extractors, _rows, _rows.Length);

        return buffer.WrittenCount;
    }

    /// <summary>
    /// Boxed/fallback path (PreferDirectStreaming=false): values are
    /// extracted into <c>object?[]</c> column arrays first, then written
    /// to the protocol buffer. This is what BulkInserter does when the
    /// direct path is unavailable (unsupported column type) or explicitly
    /// disabled.
    /// </summary>
    [Benchmark]
    public int BoxedPath()
    {
        var columnData = new object?[_columnNames.Length][];
        for (int col = 0; col < _columnNames.Length; col++)
        {
            columnData[col] = new object?[_rows.Length];
        }

        for (int row = 0; row < _rows.Length; row++)
        {
            var item = _rows[row];
            for (int col = 0; col < _columnNames.Length; col++)
            {
                columnData[col][row] = _getters[col](item);
            }
        }

        var buffer = new ArrayBufferWriter<byte>(1024 * 1024);
        var writer = new ProtocolWriter(buffer);

        WriteDataBlock(ref writer, _columnNames, _columnTypes, columnData, _rows.Length);

        return buffer.WrittenCount;
    }

    private void WriteDataBlock(
        ref ProtocolWriter writer,
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount)
    {
        writer.WriteString(string.Empty);

        writer.WriteVarInt(1);
        writer.WriteByte(0);
        writer.WriteVarInt(2);
        writer.WriteInt32(-1);
        writer.WriteVarInt(0);

        writer.WriteVarInt((ulong)columnNames.Length);
        writer.WriteVarInt((ulong)rowCount);

        for (int i = 0; i < columnNames.Length; i++)
        {
            writer.WriteString(columnNames[i]);
            writer.WriteString(columnTypes[i]);

            if (rowCount > 0)
            {
                var columnWriter = _writerRegistry.GetWriter(columnTypes[i]);
                columnWriter.WriteColumn(ref writer, columnData[i]);
            }
        }
    }

    private static void WriteDataBlockDirect<TRow>(
        ref ProtocolWriter writer,
        IColumnExtractor<TRow>[] extractors,
        TRow[] rows,
        int rowCount)
    {
        writer.WriteString(string.Empty);

        writer.WriteVarInt(1);
        writer.WriteByte(0);
        writer.WriteVarInt(2);
        writer.WriteInt32(-1);
        writer.WriteVarInt(0);

        writer.WriteVarInt((ulong)extractors.Length);
        writer.WriteVarInt((ulong)rowCount);

        for (int i = 0; i < extractors.Length; i++)
        {
            var extractor = extractors[i];
            writer.WriteString(extractor.ColumnName);
            writer.WriteString(extractor.TypeName);

            if (rowCount > 0)
            {
                extractor.ExtractAndWrite(ref writer, rows, rowCount);
            }
        }
    }

    public class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}

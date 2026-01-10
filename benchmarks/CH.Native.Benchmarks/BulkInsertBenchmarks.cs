using System.Buffers;
using System.Linq.Expressions;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using CH.Native.BulkInsert;
using CH.Native.Data;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Benchmarks for bulk insert performance - isolates the serialization path
/// without network overhead.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class BulkInsertBenchmarks
{
    private SimpleRow[] _simpleRows = null!;
    private AllTypesRow[] _allTypesRows = null!;
    private Func<SimpleRow, object?>[] _simpleGetters = null!;
    private Func<AllTypesRow, object?>[] _allTypesGetters = null!;
    private string[] _simpleColumnNames = null!;
    private string[] _simpleColumnTypes = null!;
    private string[] _allTypesColumnNames = null!;
    private string[] _allTypesColumnTypes = null!;
    private ColumnWriterRegistry _writerRegistry = null!;
    private ArrayBufferWriter<byte> _reusableBuffer = null!;

    // Column extractors for direct-to-buffer writing (no boxing)
    private IColumnExtractor<SimpleRow>[] _simpleExtractors = null!;
    private IColumnExtractor<AllTypesRow>[] _allTypesExtractors = null!;

    [Params(100, 1000, 10000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _writerRegistry = ColumnWriterRegistry.Default;
        _reusableBuffer = new ArrayBufferWriter<byte>(1024 * 1024); // 1MB initial

        // Create test data
        _simpleRows = Enumerable.Range(0, RowCount)
            .Select(i => new SimpleRow { Id = i, Name = $"Item_{i}" })
            .ToArray();

        _allTypesRows = Enumerable.Range(0, RowCount)
            .Select(i => new AllTypesRow
            {
                Id = i,
                Int8Val = (sbyte)(i % 128),
                Int16Val = (short)(i % 32000),
                Int64Val = i * 1000L,
                UInt8Val = (byte)(i % 256),
                UInt16Val = (ushort)(i % 65000),
                UInt32Val = (uint)i,
                UInt64Val = (ulong)i * 100,
                Float32Val = i * 1.5f,
                Float64Val = i * 2.5,
                Name = $"Row_{i}",
                Created = DateTime.UtcNow.AddSeconds(i)
            })
            .ToArray();

        // Create getters for SimpleRow
        _simpleGetters = new Func<SimpleRow, object?>[]
        {
            CreateGetter<SimpleRow>(typeof(SimpleRow).GetProperty("Id")!),
            CreateGetter<SimpleRow>(typeof(SimpleRow).GetProperty("Name")!)
        };
        _simpleColumnNames = new[] { "Id", "Name" };
        _simpleColumnTypes = new[] { "Int32", "String" };

        // Create getters for AllTypesRow
        var allTypesProps = typeof(AllTypesRow).GetProperties();
        _allTypesGetters = allTypesProps.Select(p => CreateGetter<AllTypesRow>(p)).ToArray();
        _allTypesColumnNames = allTypesProps.Select(p => p.Name).ToArray();
        _allTypesColumnTypes = new[]
        {
            "Int32", "Int8", "Int16", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
            "Float32", "Float64", "String", "DateTime"
        };

        // Create typed extractors for direct-to-buffer writing (no boxing)
        var simpleProps = typeof(SimpleRow).GetProperties();
        _simpleExtractors = new IColumnExtractor<SimpleRow>[]
        {
            ColumnExtractorFactory.Create<SimpleRow>(simpleProps.First(p => p.Name == "Id"), "Id", "Int32"),
            ColumnExtractorFactory.Create<SimpleRow>(simpleProps.First(p => p.Name == "Name"), "Name", "String")
        };

        _allTypesExtractors = new IColumnExtractor<AllTypesRow>[allTypesProps.Length];
        for (int i = 0; i < allTypesProps.Length; i++)
        {
            _allTypesExtractors[i] = ColumnExtractorFactory.Create<AllTypesRow>(
                allTypesProps[i],
                allTypesProps[i].Name,
                _allTypesColumnTypes[i]);
        }
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
    /// Benchmark: Extract column data from objects (current implementation - with boxing)
    /// </summary>
    [Benchmark(Baseline = true)]
    public object?[][] ExtractColumnData_WithBoxing()
    {
        var columnCount = _simpleGetters.Length;
        var columnData = new object?[columnCount][];

        for (int col = 0; col < columnCount; col++)
        {
            columnData[col] = new object?[_simpleRows.Length];
        }

        for (int row = 0; row < _simpleRows.Length; row++)
        {
            var item = _simpleRows[row];
            for (int col = 0; col < columnCount; col++)
            {
                columnData[col][row] = _simpleGetters[col](item);
            }
        }

        return columnData;
    }

    /// <summary>
    /// Benchmark: Full serialization path (extract + write) for simple rows
    /// </summary>
    [Benchmark]
    public int SerializeSimpleRows_Current()
    {
        // Extract column data (with boxing)
        var columnData = new object?[_simpleColumnNames.Length][];
        for (int col = 0; col < _simpleColumnNames.Length; col++)
        {
            columnData[col] = new object?[_simpleRows.Length];
        }

        for (int row = 0; row < _simpleRows.Length; row++)
        {
            var item = _simpleRows[row];
            for (int col = 0; col < _simpleColumnNames.Length; col++)
            {
                columnData[col][row] = _simpleGetters[col](item);
            }
        }

        // Write to buffer
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        WriteDataBlock(ref writer, _simpleColumnNames, _simpleColumnTypes, columnData, _simpleRows.Length);

        return buffer.WrittenCount;
    }

    /// <summary>
    /// Benchmark: Full serialization with reused buffer
    /// </summary>
    [Benchmark]
    public int SerializeSimpleRows_ReusedBuffer()
    {
        // Extract column data (with boxing)
        var columnData = new object?[_simpleColumnNames.Length][];
        for (int col = 0; col < _simpleColumnNames.Length; col++)
        {
            columnData[col] = new object?[_simpleRows.Length];
        }

        for (int row = 0; row < _simpleRows.Length; row++)
        {
            var item = _simpleRows[row];
            for (int col = 0; col < _simpleColumnNames.Length; col++)
            {
                columnData[col][row] = _simpleGetters[col](item);
            }
        }

        // Write to reused buffer
        _reusableBuffer.Clear();
        var writer = new ProtocolWriter(_reusableBuffer);

        WriteDataBlock(ref writer, _simpleColumnNames, _simpleColumnTypes, columnData, _simpleRows.Length);

        return _reusableBuffer.WrittenCount;
    }

    /// <summary>
    /// Benchmark: Complex row with many types
    /// </summary>
    [Benchmark]
    public int SerializeAllTypesRows_Current()
    {
        // Extract column data (with boxing)
        var columnData = new object?[_allTypesColumnNames.Length][];
        for (int col = 0; col < _allTypesColumnNames.Length; col++)
        {
            columnData[col] = new object?[_allTypesRows.Length];
        }

        for (int row = 0; row < _allTypesRows.Length; row++)
        {
            var item = _allTypesRows[row];
            for (int col = 0; col < _allTypesColumnNames.Length; col++)
            {
                columnData[col][row] = _allTypesGetters[col](item);
            }
        }

        // Write to buffer
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        WriteDataBlock(ref writer, _allTypesColumnNames, _allTypesColumnTypes, columnData, _allTypesRows.Length);

        return buffer.WrittenCount;
    }

    private void WriteDataBlock(
        ref ProtocolWriter writer,
        string[] columnNames,
        string[] columnTypes,
        object?[][] columnData,
        int rowCount)
    {
        // Table name (empty for insert data blocks)
        writer.WriteString(string.Empty);

        // Block info
        writer.WriteVarInt(1);   // field 1 marker
        writer.WriteByte(0);    // is_overflows = false
        writer.WriteVarInt(2);   // field 2 marker
        writer.WriteInt32(-1);  // bucket_num = -1
        writer.WriteVarInt(0);   // end marker

        // Column count and row count
        writer.WriteVarInt((ulong)columnNames.Length);
        writer.WriteVarInt((ulong)rowCount);

        // Write each column
        for (int i = 0; i < columnNames.Length; i++)
        {
            writer.WriteString(columnNames[i]);
            writer.WriteString(columnTypes[i]);

            // Write column data
            if (rowCount > 0)
            {
                var columnWriter = _writerRegistry.GetWriter(columnTypes[i]);
                columnWriter.WriteColumn(ref writer, columnData[i]);
            }
        }
    }

    /// <summary>
    /// Benchmark: Direct-to-buffer serialization (no boxing, no intermediate arrays)
    /// </summary>
    [Benchmark]
    public int SerializeSimpleRows_DirectPath()
    {
        _reusableBuffer.Clear();
        var writer = new ProtocolWriter(_reusableBuffer);

        WriteDataBlockDirect(ref writer, _simpleExtractors, _simpleRows, _simpleRows.Length);

        return _reusableBuffer.WrittenCount;
    }

    /// <summary>
    /// Benchmark: Direct-to-buffer serialization for complex rows
    /// </summary>
    [Benchmark]
    public int SerializeAllTypesRows_DirectPath()
    {
        _reusableBuffer.Clear();
        var writer = new ProtocolWriter(_reusableBuffer);

        WriteDataBlockDirect(ref writer, _allTypesExtractors, _allTypesRows, _allTypesRows.Length);

        return _reusableBuffer.WrittenCount;
    }

    private void WriteDataBlockDirect<TRow>(
        ref ProtocolWriter writer,
        IColumnExtractor<TRow>[] extractors,
        TRow[] rows,
        int rowCount)
    {
        // Table name (empty for insert data blocks)
        writer.WriteString(string.Empty);

        // Block info
        writer.WriteVarInt(1);   // field 1 marker
        writer.WriteByte(0);    // is_overflows = false
        writer.WriteVarInt(2);   // field 2 marker
        writer.WriteInt32(-1);  // bucket_num = -1
        writer.WriteVarInt(0);   // end marker

        // Column count and row count
        writer.WriteVarInt((ulong)extractors.Length);
        writer.WriteVarInt((ulong)rowCount);

        // Write each column directly from source data
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

    #region Test POCOs

    public class SimpleRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class AllTypesRow
    {
        public int Id { get; set; }
        public sbyte Int8Val { get; set; }
        public short Int16Val { get; set; }
        public long Int64Val { get; set; }
        public byte UInt8Val { get; set; }
        public ushort UInt16Val { get; set; }
        public uint UInt32Val { get; set; }
        public ulong UInt64Val { get; set; }
        public float Float32Val { get; set; }
        public double Float64Val { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime Created { get; set; }
    }

    #endregion
}

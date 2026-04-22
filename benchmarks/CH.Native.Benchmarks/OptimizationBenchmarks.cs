using System.Buffers;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Dynamic;
using CH.Native.Data.Json;
using CH.Native.Data.Variant;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// 100K-row micro-benchmarks targeting each optimisation in turn. No ClickHouse server
/// required — each benchmark exercises only the in-process serialisation fast path.
///
/// These sit alongside <see cref="VariantColumnBenchmarks"/> and are intentionally fixed
/// at 100K so before/after comparisons are directly legible.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 7)]
public class OptimizationBenchmarks
{
    private const int Rows = 100_000;

    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);
    private static readonly ColumnWriterFactory WriterFactory = new(ColumnWriterRegistry.Default);

    // --- Variant fixtures ---
    private ClickHouseVariant[] _variantRows = null!;
    private byte[] _variantEncoded = null!;
    private VariantColumnReader _variantReader = null!;
    private VariantColumnWriter _variantWriter = null!;
    private VariantColumnReader<long, string> _variantReaderGeneric = null!;

    // --- Dynamic fixtures (balanced 2-type) ---
    private ClickHouseDynamic[] _dynamicRows = null!;
    private byte[] _dynamicEncoded = null!;
    private DynamicColumnReader _dynamicReader = null!;
    private DynamicColumnWriter _dynamicWriter = null!;

    // --- Dynamic fixtures (max_types=1 to force shared-arm overflow) ---
    private ClickHouseDynamic[] _dynamicSharedRows = null!;
    private byte[] _dynamicSharedEncoded = null!;
    private DynamicColumnReader _dynamicSharedReader = null!;
    private DynamicColumnWriter _dynamicSharedWriter = null!;

    // --- JSON v0 binary fixture ---
    private byte[] _jsonV0Encoded = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);

        // Variant(Int64, String) with ~1/3 NULL, 1/3 Int64, 1/3 String.
        _variantReader = (VariantColumnReader)ReaderFactory.CreateReader("Variant(Int64, String)");
        _variantWriter = (VariantColumnWriter)WriterFactory.CreateWriter("Variant(Int64, String)");

        // Typed reader uses the same wire format — just decodes into VariantValue<long, string>
        // rather than the boxed ClickHouseVariant form.
        var int64Reader = (IColumnReader<long>)ColumnReaderRegistry.Default.GetReader("Int64");
        var stringReader = (IColumnReader<string>)ColumnReaderRegistry.Default.GetReader("String");
        _variantReaderGeneric = new VariantColumnReader<long, string>(int64Reader, stringReader);
        _variantRows = new ClickHouseVariant[Rows];
        for (int i = 0; i < Rows; i++)
        {
            var roll = rng.Next(3);
            _variantRows[i] = roll switch
            {
                0 => ClickHouseVariant.Null,
                1 => new ClickHouseVariant(0, (long)rng.Next()),
                _ => new ClickHouseVariant(1, $"s{rng.Next():X}"),
            };
        }
        _variantEncoded = EncodeVariant(_variantRows);

        // Dynamic balanced — arm count stays at 2, no shared overflow.
        _dynamicReader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");
        _dynamicWriter = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic");
        _dynamicRows = new ClickHouseDynamic[Rows];
        for (int i = 0; i < Rows; i++)
        {
            var roll = rng.Next(3);
            _dynamicRows[i] = roll switch
            {
                0 => ClickHouseDynamic.Null,
                1 => new ClickHouseDynamic(0, (long)rng.Next(), "Int64"),
                _ => new ClickHouseDynamic(0, $"s{rng.Next():X}", "String"),
            };
        }
        _dynamicEncoded = EncodeDynamic(_dynamicWriter, _dynamicRows);

        // Dynamic with max_types=1 — half of the rows get pushed into the shared arm.
        _dynamicSharedReader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic(max_types=1)");
        _dynamicSharedWriter = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic(max_types=1)");
        _dynamicSharedRows = new ClickHouseDynamic[Rows];
        for (int i = 0; i < Rows; i++)
        {
            // alternate Int64 (declared arm 0) and String (overflow -> shared arm)
            _dynamicSharedRows[i] = (i & 1) == 0
                ? new ClickHouseDynamic(0, (long)rng.Next(), "Int64")
                : new ClickHouseDynamic(0, $"s{rng.Next():X}", "String");
        }
        _dynamicSharedEncoded = EncodeDynamic(_dynamicSharedWriter, _dynamicSharedRows);

        // JSON v0 fixture: 4 typed paths × 100k rows.
        _jsonV0Encoded = EncodeJsonV0(Rows);
    }

    private static byte[] EncodeVariant(ClickHouseVariant[] rows)
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);
        var writer = (VariantColumnWriter)WriterFactory.CreateWriter("Variant(Int64, String)");
        writer.WriteColumn(ref w, rows);
        return buf.WrittenSpan.ToArray();
    }

    private static byte[] EncodeDynamic(DynamicColumnWriter writer, ClickHouseDynamic[] rows)
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);
        writer.WriteColumn(ref w, rows);
        return buf.WrittenSpan.ToArray();
    }

    private static byte[] EncodeJsonV0(int rowCount)
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);

        // version 0, pathCount = 4
        w.WriteUInt64(4);
        w.WriteString("user.id");
        w.WriteString("user.name");
        w.WriteString("score");
        w.WriteString("tag");
        w.WriteString("Int64");
        w.WriteString("String");
        w.WriteString("Int32");
        w.WriteString("String");

        for (int i = 0; i < rowCount; i++) w.WriteInt64(i);
        // Strings — write as protocol strings
        for (int i = 0; i < rowCount; i++) w.WriteString($"name{i}");
        for (int i = 0; i < rowCount; i++) w.WriteInt32(i);
        for (int i = 0; i < rowCount; i++) w.WriteString(i % 2 == 0 ? "even" : "odd");

        return buf.WrittenSpan.ToArray();
    }

    // --- Variant ---

    [Benchmark(Description = "Variant write 100K")]
    public int Variant_Write()
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);
        _variantWriter.WriteColumn(ref w, _variantRows);
        return buf.WrittenCount;
    }

    [Benchmark(Description = "Variant read 100K")]
    public int Variant_Read()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_variantEncoded));
        using var col = _variantReader.ReadTypedColumn(ref r, Rows);
        return col.Count;
    }

    [Benchmark(Description = "Variant<long,string> read 100K (typed)")]
    public int Variant_Read_Typed()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_variantEncoded));
        using var col = _variantReaderGeneric.ReadTypedColumn(ref r, Rows);
        return col.Count;
    }

    [Benchmark(Description = "Variant read 100K (iterate + materialise per row)")]
    public int Variant_Read_AndIterate()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_variantEncoded));
        using var col = _variantReader.ReadTypedColumn(ref r, Rows);
        long sinkLong = 0;
        int sinkStrLen = 0;
        for (int i = 0; i < col.Count; i++)
        {
            var v = (ClickHouseVariant)col.GetValue(i)!;
            if (v.IsNull) continue;
            if (v.Value is long l) sinkLong += l;
            else if (v.Value is string s) sinkStrLen += s.Length;
        }
        return (int)(sinkLong ^ sinkStrLen);
    }

    [Benchmark(Description = "Variant<long,string> read 100K (typed + iterate)")]
    public int Variant_Read_Typed_AndIterate()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_variantEncoded));
        using var col = _variantReaderGeneric.ReadTypedColumn(ref r, Rows);
        long sinkLong = 0;
        int sinkStrLen = 0;
        var span = col.Values;
        for (int i = 0; i < span.Length; i++)
        {
            var v = span[i];
            if (v.IsNull) continue;
            if (v.Discriminator == 0) sinkLong += v.Arm0;
            else sinkStrLen += v.Arm1.Length;
        }
        return (int)(sinkLong ^ sinkStrLen);
    }

    // --- Dynamic (balanced) ---

    [Benchmark(Description = "Dynamic write 100K")]
    public int Dynamic_Write()
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);
        _dynamicWriter.WriteColumn(ref w, _dynamicRows);
        return buf.WrittenCount;
    }

    [Benchmark(Description = "Dynamic read 100K")]
    public int Dynamic_Read()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_dynamicEncoded));
        using var col = _dynamicReader.ReadTypedColumn(ref r, Rows);
        return col.Count;
    }

    // --- Dynamic (shared-arm overflow, max_types=1) ---

    [Benchmark(Description = "Dynamic(max=1) write 100K [shared arm]")]
    public int DynamicShared_Write()
    {
        using var buf = new PooledBufferWriter();
        var w = new ProtocolWriter(buf);
        _dynamicSharedWriter.WriteColumn(ref w, _dynamicSharedRows);
        return buf.WrittenCount;
    }

    [Benchmark(Description = "Dynamic(max=1) read 100K [shared arm]")]
    public int DynamicShared_Read()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_dynamicSharedEncoded));
        using var col = _dynamicSharedReader.ReadTypedColumn(ref r, Rows);
        return col.Count;
    }

    // --- JSON v0 binary decode ---

    [Benchmark(Description = "JSON v0 decode 100K")]
    public int Json_V0_Decode()
    {
        var r = new ProtocolReader(new ReadOnlySequence<byte>(_jsonV0Encoded));
        var docs = JsonBinaryDecoder.DecodeVersion0(ref r, Rows, ReaderFactory);
        for (int i = 0; i < docs.Length; i++) docs[i].Dispose();
        return docs.Length;
    }
}

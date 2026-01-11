using System.Buffers;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Protocol;

namespace CH.Native.Benchmarks;

/// <summary>
/// Benchmarks for JSON column reader/writer/skipper performance.
/// These are unit-level benchmarks that don't require a live ClickHouse server.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class JsonColumnBenchmarks
{
    private byte[] _simpleJsonBytes = null!;
    private byte[] _complexJsonBytes = null!;
    private byte[] _multiRowJsonBytes = null!;
    private JsonDocument _simpleDoc = null!;
    private JsonDocument _complexDoc = null!;

    private const string SimpleJson = """{"name":"Alice","age":30}""";
    private const string ComplexJson = """{"user":{"id":12345,"profile":{"name":"Alice","email":"alice@example.com","settings":{"theme":"dark","notifications":true}}},"tags":["admin","user","verified"],"metadata":{"created":"2024-01-15T10:30:00Z","updated":"2024-06-20T15:45:00Z"}}""";

    [GlobalSetup]
    public void Setup()
    {
        // Create protocol-formatted bytes (length-prefixed strings)
        _simpleJsonBytes = CreateProtocolBytes(SimpleJson);
        _complexJsonBytes = CreateProtocolBytes(ComplexJson);
        _multiRowJsonBytes = CreateMultiRowProtocolBytes(SimpleJson, 100);

        _simpleDoc = JsonDocument.Parse(SimpleJson);
        _complexDoc = JsonDocument.Parse(ComplexJson);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _simpleDoc.Dispose();
        _complexDoc.Dispose();
    }

    private static byte[] CreateProtocolBytes(string json)
    {
        var jsonBytes = Encoding.UTF8.GetBytes(json);
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteString(json);
        return buffer.WrittenSpan.ToArray();
    }

    private static byte[] CreateMultiRowProtocolBytes(string json, int rowCount)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        for (int i = 0; i < rowCount; i++)
        {
            writer.WriteString(json);
        }
        return buffer.WrittenSpan.ToArray();
    }

    // --- JsonColumnReader Benchmarks ---

    [Benchmark(Description = "Read simple JSON")]
    public JsonDocument Reader_SimpleJson()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var columnReader = new JsonColumnReader();
        var doc = columnReader.ReadValue(ref reader);
        doc.Dispose();
        return doc;
    }

    [Benchmark(Description = "Read complex JSON")]
    public JsonDocument Reader_ComplexJson()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_complexJsonBytes));
        var columnReader = new JsonColumnReader();
        var doc = columnReader.ReadValue(ref reader);
        doc.Dispose();
        return doc;
    }

    [Benchmark(Description = "Read 100 JSON rows")]
    public int Reader_MultiRow()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiRowJsonBytes));
        var columnReader = new JsonColumnReader();
        using var column = columnReader.ReadTypedColumn(ref reader, 100);

        // Dispose all documents
        for (int i = 0; i < column.Count; i++)
        {
            column[i].Dispose();
        }

        return column.Count;
    }

    // --- JsonStringColumnReader Benchmarks (no parsing) ---

    [Benchmark(Description = "Read simple JSON as string")]
    public string Reader_SimpleJson_AsString()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var columnReader = new JsonStringColumnReader();
        return columnReader.ReadValue(ref reader);
    }

    [Benchmark(Description = "Read complex JSON as string")]
    public string Reader_ComplexJson_AsString()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_complexJsonBytes));
        var columnReader = new JsonStringColumnReader();
        return columnReader.ReadValue(ref reader);
    }

    // --- JsonColumnWriter Benchmarks ---

    [Benchmark(Description = "Write simple JSON")]
    public int Writer_SimpleJson()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var columnWriter = new JsonColumnWriter();
        columnWriter.WriteValue(ref writer, _simpleDoc);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Write complex JSON")]
    public int Writer_ComplexJson()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var columnWriter = new JsonColumnWriter();
        columnWriter.WriteValue(ref writer, _complexDoc);
        return buffer.WrittenCount;
    }

    [Benchmark(Description = "Write JSON from string")]
    public int Writer_FromString()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var columnWriter = new JsonColumnWriter();
        ((IColumnWriter)columnWriter).WriteValue(ref writer, SimpleJson);
        return buffer.WrittenCount;
    }

    // --- JsonColumnSkipper Benchmarks ---

    [Benchmark(Description = "Skip simple JSON")]
    public bool Skipper_SimpleJson()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var skipper = new JsonColumnSkipper();
        return skipper.TrySkipColumn(ref reader, 1);
    }

    [Benchmark(Description = "Skip 100 JSON rows")]
    public bool Skipper_MultiRow()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiRowJsonBytes));
        var skipper = new JsonColumnSkipper();
        return skipper.TrySkipColumn(ref reader, 100);
    }

    // --- Comparison: JsonDocument vs String ---

    [Benchmark(Description = "Parse then access property")]
    public string? ParseAndAccess()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var columnReader = new JsonColumnReader();
        using var doc = columnReader.ReadValue(ref reader);
        return doc.RootElement.GetProperty("name").GetString();
    }

    [Benchmark(Description = "Read as string only")]
    public string ReadStringOnly()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var columnReader = new JsonStringColumnReader();
        return columnReader.ReadValue(ref reader);
    }
}

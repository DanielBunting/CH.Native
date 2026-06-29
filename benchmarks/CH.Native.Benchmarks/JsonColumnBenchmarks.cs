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
    private byte[] _simpleJsonColumnBytes = null!;
    private byte[] _multiRowJsonColumnBytes = null!;
    private JsonDocument _simpleDoc = null!;
    private JsonDocument _complexDoc = null!;

    private const string SimpleJson = """{"name":"Alice","age":30}""";
    private const string ComplexJson = """{"user":{"id":12345,"profile":{"name":"Alice","email":"alice@example.com","settings":{"theme":"dark","notifications":true}}},"tags":["admin","user","verified"],"metadata":{"created":"2024-01-15T10:30:00Z","updated":"2024-06-20T15:45:00Z"}}""";

    [GlobalSetup]
    public void Setup()
    {
        // Bare value bytes (a single length-prefixed string) for the ReadValue path.
        _simpleJsonBytes = CreateProtocolBytes(SimpleJson);
        _complexJsonBytes = CreateProtocolBytes(ComplexJson);
        // Full column wire format (UInt64 version prefix + rows) for the ReadPrefix +
        // ReadTypedColumn and skip paths, which read the serialization version first.
        _simpleJsonColumnBytes = CreateJsonColumnBytes(SimpleJson, 1);
        _multiRowJsonColumnBytes = CreateJsonColumnBytes(SimpleJson, 100);

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

    // Full JSON column wire format: a UInt64 serialization version (1 = string) followed
    // by rowCount length-prefixed UTF-8 strings — i.e. exactly what ReadPrefix +
    // ReadTypedColumn (and the skipper's TrySkipColumn) consume.
    private static byte[] CreateJsonColumnBytes(string json, int rowCount)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1);
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
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiRowJsonColumnBytes));
        var columnReader = new JsonColumnReader();
        columnReader.ReadPrefix(ref reader);
        using var column = columnReader.ReadTypedColumn(ref reader, 100);

        // Dispose all documents
        for (int i = 0; i < column.Count; i++)
        {
            column[i].Dispose();
        }

        return column.Count;
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
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonColumnBytes));
        var skipper = new JsonColumnSkipper();
        return skipper.TrySkipColumn(ref reader, 1);
    }

    [Benchmark(Description = "Skip 100 JSON rows")]
    public bool Skipper_MultiRow()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_multiRowJsonColumnBytes));
        var skipper = new JsonColumnSkipper();
        return skipper.TrySkipColumn(ref reader, 100);
    }

    // --- Comparison ---

    [Benchmark(Description = "Parse then access property")]
    public string? ParseAndAccess()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(_simpleJsonBytes));
        var columnReader = new JsonColumnReader();
        using var doc = columnReader.ReadValue(ref reader);
        return doc.RootElement.GetProperty("name").GetString();
    }
}

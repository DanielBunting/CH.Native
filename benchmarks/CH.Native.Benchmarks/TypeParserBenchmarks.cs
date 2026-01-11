using BenchmarkDotNet.Attributes;
using CH.Native.Data.Types;

namespace CH.Native.Benchmarks;

/// <summary>
/// Benchmarks for ClickHouseTypeParser performance.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class TypeParserBenchmarks
{
    // Simple types
    [Benchmark]
    public ClickHouseType Parse_SimpleType_Int32()
        => ClickHouseTypeParser.Parse("Int32");

    [Benchmark]
    public ClickHouseType Parse_SimpleType_String()
        => ClickHouseTypeParser.Parse("String");

    // Single parameter types
    [Benchmark]
    public ClickHouseType Parse_Nullable_Int32()
        => ClickHouseTypeParser.Parse("Nullable(Int32)");

    [Benchmark]
    public ClickHouseType Parse_Array_String()
        => ClickHouseTypeParser.Parse("Array(String)");

    [Benchmark]
    public ClickHouseType Parse_FixedString_32()
        => ClickHouseTypeParser.Parse("FixedString(32)");

    [Benchmark]
    public ClickHouseType Parse_DateTime64_WithTimezone()
        => ClickHouseTypeParser.Parse("DateTime64(3, 'UTC')");

    // Complex nested types
    [Benchmark]
    public ClickHouseType Parse_Nullable_Array_Int32()
        => ClickHouseTypeParser.Parse("Nullable(Array(Int32))");

    [Benchmark]
    public ClickHouseType Parse_Map_String_Int32()
        => ClickHouseTypeParser.Parse("Map(String, Int32)");

    [Benchmark]
    public ClickHouseType Parse_Map_Complex()
        => ClickHouseTypeParser.Parse("Map(String, Array(Nullable(Int32)))");

    // Named tuples
    [Benchmark]
    public ClickHouseType Parse_NamedTuple_Simple()
        => ClickHouseTypeParser.Parse("Tuple(id UInt64, name String)");

    [Benchmark]
    public ClickHouseType Parse_NamedTuple_Complex()
        => ClickHouseTypeParser.Parse("Tuple(id UInt64, tags Array(String), metadata Nullable(String))");

    // Nested type
    [Benchmark]
    public ClickHouseType Parse_Nested()
        => ClickHouseTypeParser.Parse("Nested(id UInt64, name String, values Array(Int32))");

    // Enum types (complex parameter parsing)
    [Benchmark]
    public ClickHouseType Parse_Enum8()
        => ClickHouseTypeParser.Parse("Enum8('active' = 1, 'inactive' = 0, 'pending' = 2)");

    // Decimal types
    [Benchmark]
    public ClickHouseType Parse_Decimal128()
        => ClickHouseTypeParser.Parse("Decimal128(18)");

    [Benchmark]
    public ClickHouseType Parse_Decimal_PrecisionScale()
        => ClickHouseTypeParser.Parse("Decimal(38, 10)");

    // LowCardinality
    [Benchmark]
    public ClickHouseType Parse_LowCardinality_String()
        => ClickHouseTypeParser.Parse("LowCardinality(String)");

    [Benchmark]
    public ClickHouseType Parse_LowCardinality_Nullable_String()
        => ClickHouseTypeParser.Parse("LowCardinality(Nullable(String))");

    // Worst case - deeply nested
    [Benchmark]
    public ClickHouseType Parse_DeeplyNested()
        => ClickHouseTypeParser.Parse("Map(String, Array(Tuple(id Nullable(UInt64), data LowCardinality(String))))");

    // JSON types
    [Benchmark]
    public ClickHouseType Parse_JSON_Simple()
        => ClickHouseTypeParser.Parse("JSON");

    [Benchmark]
    public ClickHouseType Parse_JSON_WithParams()
        => ClickHouseTypeParser.Parse("JSON(max_dynamic_paths=100)");

    [Benchmark]
    public ClickHouseType Parse_Nullable_JSON()
        => ClickHouseTypeParser.Parse("Nullable(JSON)");

    [Benchmark]
    public ClickHouseType Parse_Array_JSON()
        => ClickHouseTypeParser.Parse("Array(JSON)");
}

using System.Buffers;
using System.Numerics;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Geo;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Single parameterised round-trip across every primitive ClickHouse type the
/// driver supports as a writer. Each row of the theory writes a representative
/// payload via the canonical writer and reads it back via the canonical reader,
/// asserting full equality. Catches drift between reader and writer changes
/// that pass independently in their per-type unit tests.
/// </summary>
public class ColumnSymmetryTheoryTests
{
    public static IEnumerable<object[]> Symmetric()
    {
        yield return Case("Int8", new sbyte[] { sbyte.MinValue, -1, 0, 1, sbyte.MaxValue },
            new Int8ColumnWriter(), new Int8ColumnReader());
        yield return Case("Int16", new short[] { short.MinValue, -1, 0, 1, short.MaxValue },
            new Int16ColumnWriter(), new Int16ColumnReader());
        yield return Case("Int32", new[] { int.MinValue, -1, 0, 1, int.MaxValue },
            new Int32ColumnWriter(), new Int32ColumnReader());
        yield return Case("Int64", new[] { long.MinValue, -1L, 0L, 1L, long.MaxValue },
            new Int64ColumnWriter(), new Int64ColumnReader());
        yield return Case("Int128", new[] { Int128.MinValue, Int128.NegativeOne, Int128.Zero, Int128.MaxValue },
            new Int128ColumnWriter(), new Int128ColumnReader());
        yield return Case("Int256", new[] { BigInteger.Zero, BigInteger.One, BigInteger.MinusOne, BigInteger.Pow(2, 200) },
            new Int256ColumnWriter(), new Int256ColumnReader());
        yield return Case("UInt8", new byte[] { 0, 1, 127, 128, 255 },
            new UInt8ColumnWriter(), new UInt8ColumnReader());
        yield return Case("UInt16", new ushort[] { 0, 1, ushort.MaxValue },
            new UInt16ColumnWriter(), new UInt16ColumnReader());
        yield return Case("UInt32", new uint[] { 0u, 1u, uint.MaxValue },
            new UInt32ColumnWriter(), new UInt32ColumnReader());
        yield return Case("UInt64", new ulong[] { 0ul, 1ul, ulong.MaxValue },
            new UInt64ColumnWriter(), new UInt64ColumnReader());
        yield return Case("UInt128", new[] { UInt128.Zero, UInt128.One, UInt128.MaxValue },
            new UInt128ColumnWriter(), new UInt128ColumnReader());
        yield return Case("UInt256", new[] { BigInteger.Zero, (BigInteger)ulong.MaxValue + 1, BigInteger.Pow(2, 256) - 1 },
            new UInt256ColumnWriter(), new UInt256ColumnReader());
        yield return Case("Float32", new[] { -1f, 0f, 1f, float.MaxValue, float.MinValue, float.Epsilon },
            new Float32ColumnWriter(), new Float32ColumnReader());
        yield return Case("Float64", new[] { -Math.E, 0.0, Math.PI, double.MaxValue, double.MinValue },
            new Float64ColumnWriter(), new Float64ColumnReader());
        yield return Case("Bool", new[] { true, false, true, true, false },
            new BoolColumnWriter(), new BoolColumnReader());
        yield return Case("Date32", new[] { new DateOnly(1970, 1, 1), new DateOnly(1900, 1, 1), new DateOnly(2299, 12, 31) },
            new Date32ColumnWriter(), new Date32ColumnReader());
        yield return Case("DateTime", new[]
            {
                new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2024, 6, 15, 12, 30, 45, DateTimeKind.Utc),
                new DateTime(2106, 2, 7, 6, 28, 15, DateTimeKind.Utc),
            },
            new DateTimeColumnWriter(), new DateTimeColumnReader());
        yield return Case("DateTimeWithTimezone", new[]
            {
                new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero),
                new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero),
            },
            new DateTimeWithTimezoneColumnWriter("UTC"),
            new DateTimeWithTimezoneColumnReader("UTC"));
        yield return Case("Enum8", new sbyte[] { -128, -1, 0, 1, 127 },
            new Enum8ColumnWriter(), new Enum8ColumnReader());
        yield return Case("Enum16", new short[] { -32000, 0, 32000 },
            new Enum16ColumnWriter(), new Enum16ColumnReader());
        yield return Case("String", new[] { "", "ascii", "你好世界", "🎉", new string('x', 10_000) },
            new StringColumnWriter(), new StringColumnReader());
        yield return Case("UUID", new[] { Guid.Empty, new Guid("550e8400-e29b-41d4-a716-446655440000"), Guid.NewGuid() },
            new UuidColumnWriter(), new UuidColumnReader());
        yield return Case("IPv4",
            new[]
            {
                System.Net.IPAddress.Parse("0.0.0.0"),
                System.Net.IPAddress.Parse("127.0.0.1"),
                System.Net.IPAddress.Parse("255.255.255.255"),
            },
            new IPv4ColumnWriter(), new IPv4ColumnReader());
        yield return Case("IPv6",
            new[]
            {
                System.Net.IPAddress.Parse("::1"),
                System.Net.IPAddress.Parse("2001:db8::1"),
                System.Net.IPAddress.Parse("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
            },
            new IPv6ColumnWriter(), new IPv6ColumnReader());
        yield return Case("Decimal32(4)", new[] { 0m, 99.9999m, -99.9999m },
            new Decimal32ColumnWriter(4), new Decimal32ColumnReader(4));
        yield return Case("Decimal64(8)", new[] { 0m, 12345.12345678m, -999.99999999m },
            new Decimal64ColumnWriter(8), new Decimal64ColumnReader(8));
        // Decimal128/256 use ClickHouseDecimal (not decimal); covered separately
        // in DecimalScaleRoundTripTests for the wide cases.
        yield return Case("Point",
            new[] { new Point(1, 2), Point.Zero, new Point(-3.5, 4.25) },
            new PointColumnWriter(), new PointColumnReader());
        yield return Case("BFloat16", new[] { 1.0f, -1.0f, 2.0f, float.PositiveInfinity },
            new BFloat16ColumnWriter(), new BFloat16ColumnReader());

        yield return Case<int[]>("Array(Int32)",
            new[] { new[] { 1, 2, 3 }, Array.Empty<int>(), new[] { 42 } },
            new ArrayColumnWriter<int>(new Int32ColumnWriter()),
            new ArrayColumnReader<int>(new Int32ColumnReader()));
        yield return Case<int?>("Nullable(Int32)",
            new int?[] { 1, null, 2, null, 3 },
            new NullableColumnWriter<int>(new Int32ColumnWriter()),
            new NullableColumnReader<int>(new Int32ColumnReader()));
        yield return Case<DateTime?>("Nullable(DateTime)",
            new DateTime?[]
            {
                new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                null,
                new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            },
            new NullableColumnWriter<DateTime>(new DateTimeColumnWriter()),
            new NullableColumnReader<DateTime>(new DateTimeColumnReader()));
    }

    [Theory]
    [MemberData(nameof(Symmetric))]
    public void RoundTrip_TypeWritesAndReadsBack(string label, IRoundTripCase @case)
    {
        // The label parameter is here so xUnit shows the type in test output
        // ("Int128" instead of an opaque MemberData index).
        Assert.NotNull(label);
        @case.Run();
    }

    public interface IRoundTripCase
    {
        void Run();
    }

    private sealed class RoundTripCase<T> : IRoundTripCase
    {
        public required T[] Values { get; init; }
        public required IColumnWriter<T> Writer { get; init; }
        public required IColumnReader<T> Reader { get; init; }

        public void Run()
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new ProtocolWriter(buffer);
            Writer.WriteColumn(ref writer, Values);

            var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            using var column = Reader.ReadTypedColumn(ref reader, Values.Length);

            Assert.Equal(Values.Length, column.Count);
            for (int i = 0; i < Values.Length; i++)
                Assert.Equal(Values[i], column[i]);
        }
    }

    private static object[] Case<T>(string label, T[] values, IColumnWriter<T> writer, IColumnReader<T> reader)
    {
        return new object[]
        {
            label,
            new RoundTripCase<T> { Values = values, Writer = writer, Reader = reader },
        };
    }
}

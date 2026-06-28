using System.Buffers;
using System.Collections;
using CH.Native.Data;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Wire round-trip tests for the Nested(...) reader/writer pair (release-prep item 2).
/// Writes a Nested column with <see cref="ColumnWriters.NestedColumnWriter"/> and reads
/// it back with <see cref="ColumnReaders.NestedColumnReader"/>, asserting the per-row
/// field arrays survive. Both sides must use the shared-offsets wire layout ClickHouse
/// uses (one offsets block, then each field's flat values) — verified end-to-end by
/// <c>BulkInsertNestedTypeTests</c>; these pin the same contract without a server.
/// </summary>
/// <remarks>
/// Note: this catches the reader and writer <i>disagreeing</i>. It would not, on its own,
/// catch them being wrong in the same way (the original pre-fix state). The Docker
/// integration test is the source of truth for the wire format; this guards regressions
/// cheaply once the format is right.
/// </remarks>
public class NestedColumnRoundTripTests
{
    private static object[][] RoundTrip(string nestedType, object[][] rows)
    {
        var writer = ColumnWriterRegistry.Default.GetWriter(nestedType);
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WritePrefix(ref pw);
        writer.WriteColumn(ref pw, rows);

        var reader = ColumnReaderRegistry.Default.GetReader(nestedType);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, rows.Length);

        var result = new object[rows.Length][];
        for (int r = 0; r < rows.Length; r++)
            result[r] = (object[])col.GetValue(r)!;
        return result;
    }

    private static void AssertRowsEqual(object[][] expected, object[][] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int r = 0; r < expected.Length; r++)
        {
            Assert.Equal(expected[r].Length, actual[r].Length);
            for (int f = 0; f < expected[r].Length; f++)
            {
                var exp = ((IEnumerable)expected[r][f]).Cast<object>().ToArray();
                var act = ((IEnumerable)actual[r][f]).Cast<object>().ToArray();
                Assert.Equal(exp, act);
            }
        }
    }

    [Fact]
    public void TwoFields_StringInt_RoundTrips()
    {
        var rows = new[]
        {
            new object[] { new[] { "a", "b" }, new[] { 10, 20 } },
            new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() }, // empty row
            new object[] { new[] { "only" }, new[] { 99 } },
        };

        var result = RoundTrip("Nested(key String, value Int32)", rows);
        AssertRowsEqual(rows, result);
    }

    [Fact]
    public void SingleField_RoundTrips()
    {
        var rows = new[]
        {
            new object[] { new[] { 1, 2, 3 } },
            new object[] { System.Array.Empty<int>() },
            new object[] { new[] { 7 } },
        };

        var result = RoundTrip("Nested(x Int32)", rows);
        AssertRowsEqual(rows, result);
    }

    [Fact]
    public void ThreeFields_MixedTypes_RoundTrips()
    {
        var rows = new[]
        {
            new object[] { new[] { "x", "y" }, new[] { 1L, 2L }, new[] { 1.5, 2.5 } },
            new object[] { new[] { "z" }, new[] { 9L }, new[] { 9.9 } },
            new object[] { System.Array.Empty<string>(), System.Array.Empty<long>(), System.Array.Empty<double>() },
        };

        var result = RoundTrip("Nested(name String, id Int64, score Float64)", rows);
        AssertRowsEqual(rows, result);
    }

    [Fact]
    public void AllRowsEmpty_RoundTrips()
    {
        var rows = new[]
        {
            new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() },
            new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() },
        };

        var result = RoundTrip("Nested(key String, value Int32)", rows);
        AssertRowsEqual(rows, result);
    }

    [Fact]
    public void ManyRows_VaryingLengths_RoundTrips()
    {
        var rows = new object[40][];
        for (int i = 0; i < rows.Length; i++)
        {
            int len = i % 5; // 0..4 elements per row, including empties
            var keys = new string[len];
            var vals = new int[len];
            for (int j = 0; j < len; j++)
            {
                keys[j] = $"r{i}_{j}";
                vals[j] = i * 100 + j;
            }
            rows[i] = new object[] { keys, vals };
        }

        var result = RoundTrip("Nested(key String, value Int32)", rows);
        AssertRowsEqual(rows, result);
    }

    [Fact]
    public void ZeroRows_RoundTrips()
    {
        var rows = System.Array.Empty<object[]>();
        var result = RoundTrip("Nested(key String, value Int32)", rows);
        Assert.Empty(result);
    }

    [Fact]
    public void Skipper_ConsumesExactBytes_SentinelColumnStillReadable()
    {
        // The skipper must consume exactly the bytes the writer emitted for a Nested
        // column (shared offsets once + each field's flat values). If it reads per-field
        // offsets, it misaligns the stream and the trailing sentinel column reads garbage
        // (and the real connection would be poisoned).
        const string nestedType = "Nested(key String, value Int32)";
        var nestedRows = new[]
        {
            new object[] { new[] { "a", "b" }, new[] { 10, 20 } },
            new object[] { System.Array.Empty<string>(), System.Array.Empty<int>() },
            new object[] { new[] { "only" }, new[] { 99 } },
        };
        const int sentinel = 1234567;

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);

        var nestedWriter = ColumnWriterRegistry.Default.GetWriter(nestedType);
        nestedWriter.WritePrefix(ref pw);
        nestedWriter.WriteColumn(ref pw, nestedRows);

        // A trailing Int32 column (one row) acts as the alignment sentinel.
        ColumnWriterRegistry.Default.GetWriter("Int32").WriteColumn(ref pw, new object?[] { sentinel });

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        var skipper = ColumnSkipperRegistry.Default.GetSkipper(nestedType);
        Assert.True(skipper.TrySkipColumn(ref pr, nestedRows.Length));

        using var sentinelCol = ColumnReaderRegistry.Default.GetReader("Int32").ReadTypedColumn(ref pr, 1);
        Assert.Equal(sentinel, sentinelCol.GetValue(0));
    }
}

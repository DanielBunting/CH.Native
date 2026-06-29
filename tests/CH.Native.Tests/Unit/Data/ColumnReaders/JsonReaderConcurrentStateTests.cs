using System.Buffers;
using System.Text;
using System.Text.Json;
using CH.Native.Data;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Regression guard for the shared-mutable-state hazard in the JSON column reader.
///
/// <para>
/// <see cref="ColumnReaderRegistry.Default"/> is a process-wide singleton, and the
/// block-read loop (<c>Block.ReadColumnsWithHeader</c>) obtains a reader per column via
/// <c>registry.GetReader(typeName)</c>, then calls <c>ReadPrefix</c> immediately followed
/// by <c>ReadTypedColumn</c>. The JSON reader records the wire serialization version in
/// <c>ReadPrefix</c> and dispatches on it in <c>ReadTypedColumn</c>. If two concurrent
/// queries (different pooled connections, different threads) share one reader instance,
/// query B's <c>ReadPrefix</c> can clobber the version between query A's <c>ReadPrefix</c>
/// and <c>ReadTypedColumn</c>, causing A to decode with the wrong sub-format.
/// </para>
///
/// <para>
/// This test reproduces that interleave deterministically — no threads, no server — by
/// driving the registry-obtained reader(s) in the exact order the block loop would, with
/// two columns whose serialization versions differ. It is intentionally agnostic to the
/// fix mechanism (per-query reader instances OR a stateless reader): both make query A
/// decode its own column correctly.
/// </para>
/// </summary>
public class JsonReaderConcurrentStateTests
{
    private const ulong JsonStringSerializationVersion = 1;
    private const ulong JsonDeprecatedObjectSerializationVersion = 0;

    // A JSON column on the wire: UInt64 serialization version, then per row a
    // varint-length-prefixed UTF-8 string (version 1 / string serialization).
    private static byte[] JsonV1Column(string json)
    {
        var jsonBytes = Encoding.UTF8.GetBytes(json);
        var result = new byte[8 + VarIntLength(jsonBytes.Length) + jsonBytes.Length];
        var span = result.AsSpan();

        BitConverter.TryWriteBytes(span, JsonStringSerializationVersion);
        var offset = 8 + WriteVarInt(span.Slice(8), jsonBytes.Length);
        jsonBytes.CopyTo(span.Slice(offset));
        return result;
    }

    // Only the column-state prefix is needed to clobber a shared reader's version field:
    // a bare UInt64 advertising the deprecated object (binary, version 0) serialization.
    private static byte[] JsonPrefixOnly(ulong version)
    {
        var result = new byte[8];
        BitConverter.TryWriteBytes(result.AsSpan(), version);
        return result;
    }

    private static int VarIntLength(int value)
    {
        int length = 0;
        do { length++; value >>= 7; } while (value > 0);
        return length;
    }

    private static int WriteVarInt(Span<byte> span, int value)
    {
        int written = 0;
        do
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value > 0) b |= 0x80;
            span[written++] = b;
        } while (value > 0);
        return written;
    }

    [Fact]
    public void InterleavedJsonColumnReads_FromSharedRegistry_DoNotCorruptEachOther()
    {
        var registry = ColumnReaderRegistry.Default;

        // Two queries, each taking a JSON reader from the shared registry exactly as the
        // block-read loop does.
        var readerA = registry.GetReader("JSON");
        var readerB = registry.GetReader("JSON");

        // Query A: a normal version-1 (string) JSON column.
        var stateA = new ProtocolReader(new ReadOnlySequence<byte>(JsonV1Column(@"{""q"":""A""}")));
        // Query B: a column whose state prefix advertises a DIFFERENT serialization
        // version (0). Only its prefix is consumed here.
        var stateB = new ProtocolReader(new ReadOnlySequence<byte>(JsonPrefixOnly(JsonDeprecatedObjectSerializationVersion)));

        // Interleave precisely the way two concurrent block reads can:
        //   A.ReadPrefix  ->  B.ReadPrefix  ->  A.ReadTypedColumn
        readerA.ReadPrefix(ref stateA);
        readerB.ReadPrefix(ref stateB);

        // Query A must still decode its own version-1 column. With a shared, stateful
        // reader instance, A now sees B's version (0) and mis-dispatches to the binary
        // decoder, throwing / corrupting. With the fix, A reads "A" correctly.
        var columnA = readerA.ReadTypedColumn(ref stateA, 1);

        Assert.Equal(1, columnA.Count);
        var doc = Assert.IsType<JsonDocument>(columnA.GetValue(0));
        using (doc)
        {
            Assert.Equal("A", doc.RootElement.GetProperty("q").GetString());
        }
    }
}

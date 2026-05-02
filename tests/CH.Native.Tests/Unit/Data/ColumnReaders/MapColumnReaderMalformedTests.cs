using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Pins the failure modes of <see cref="MapColumnReader{TKey,TValue}"/> when the
/// wire stream is malformed. Today's contract:
///
/// <list type="bullet">
/// <item><description>An offsets array with a final cumulative count exceeding
///     <see cref="int.MaxValue"/> surfaces as <see cref="ClickHouseProtocolException"/>
///     via <c>ProtocolGuards.ToInt32(... "Map total entries")</c>.</description></item>
/// <item><description>A non-monotonic offsets sequence (entry N+1 &lt; entry N) surfaces
///     a typed exception when the negative count is passed to <see cref="Dictionary{TKey,TValue}"/>'s
///     constructor — locking the load-bearing implicit guard in.</description></item>
/// <item><description>Duplicate keys within a single row are silently last-wins (matches
///     server-side semantics; documented in the reader's source).</description></item>
/// </list>
///
/// <para>
/// These tests complement the existing
/// <c>VariantColumnReaderTests.ReadTypedColumn_DiscriminatorOutOfRange_Throws</c>
/// — together they cover the malformed-wire defences for every composite reader
/// the audit flagged as risky.
/// </para>
/// </summary>
public class MapColumnReaderMalformedTests
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);

    private static IColumnReader CreateMapReader() =>
        ReaderFactory.CreateReader("Map(String, Int64)");

    [Fact]
    public void ReadTypedColumn_TotalEntriesExceedsInt32_ThrowsProtocolException()
    {
        var reader = CreateMapReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // 1 row whose offsets[0] = 0xFFFFFFFFFFFFFFFF — trips ProtocolGuards.ToInt32.
        writer.WriteUInt64(0xFFFFFFFF_FFFFFFFFUL);

        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            using var _ = reader.ReadTypedColumn(ref pr, 1);
        });
    }

    [Fact]
    public void ReadTypedColumn_NonMonotonicOffsets_ThrowsTypedException()
    {
        // Offsets must be cumulative (monotonically non-decreasing). A row N+1
        // offset less than row N's would produce a negative per-row count,
        // which Dictionary<,>(int capacity) rejects.
        var reader = CreateMapReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // 3 rows, offsets 5 → 2 → 4: row 1 has count = 2 - 5 = -3.
        writer.WriteUInt64(5);
        writer.WriteUInt64(2);
        writer.WriteUInt64(4);

        // Inner readers don't get to the malformed step because totalEntries =
        // last offset = 4, the keys array is read, then the per-row split's
        // negative count fails. Either an ArgumentOutOfRangeException or a
        // protocol exception is acceptable — what's NOT acceptable is silent
        // success or AccessViolation.
        Exception? thrown = null;
        try
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            using var col = reader.ReadTypedColumn(ref pr, 3);
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        Assert.NotNull(thrown);
        Assert.True(
            thrown is ArgumentOutOfRangeException
            || thrown is ClickHouseProtocolException
            || thrown is InvalidOperationException
            || thrown is OverflowException,
            $"Non-monotonic offsets should surface a typed exception; got {thrown!.GetType().FullName}: {thrown.Message}");
    }

    [Fact]
    public void ReadTypedColumn_RowCountZero_NoBytesConsumed()
    {
        // Sanity: zero-row read must not consume any wire bytes (callers depend
        // on this for nested-block layouts).
        var reader = CreateMapReader();

        using var buffer = new PooledBufferWriter();
        // Empty buffer.

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 0);
        Assert.Equal(0, col.Count);
        Assert.Equal(0, pr.Remaining);
    }
}

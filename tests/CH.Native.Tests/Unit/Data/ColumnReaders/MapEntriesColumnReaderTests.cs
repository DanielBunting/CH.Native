using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Tests for <see cref="MapEntriesColumnReader{TKey, TValue}"/>, the lossless
/// reader that materialises <c>Map(K, V)</c> columns as
/// <see cref="ClickHouseMap{TKey, TValue}"/> preserving duplicates and order.
///
/// Wire format matches <see cref="MapColumnReader{TKey, TValue}"/>: per-row UInt64
/// cumulative offsets, then a flat keys column, then a flat values column.
/// </summary>
public class MapEntriesColumnReaderTests
{
    private static MapEntriesColumnReader<string, int> CreateReader()
    {
        var registry = ColumnReaderRegistry.Default;
        var keyReader = (IColumnReader<string>)registry.GetReader("String");
        var valueReader = (IColumnReader<int>)registry.GetReader("Int32");
        return new MapEntriesColumnReader<string, int>(keyReader, valueReader);
    }

    private static void WriteString(ProtocolWriter writer, string s) => writer.WriteString(s);

    [Fact]
    public void ClrType_IsClickHouseMap()
    {
        var reader = CreateReader();
        Assert.Equal(typeof(ClickHouseMap<string, int>), reader.ClrType);
    }

    [Fact]
    public void TypeName_MatchesUnderlyingMapShape()
    {
        var reader = CreateReader();
        Assert.Equal("Map(String, Int32)", reader.TypeName);
    }

    [Fact]
    public void ReadValue_DuplicateKeys_PreservedInOrder()
    {
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // Single value: count = 3, then 3 keys, then 3 values.
        writer.WriteUInt64(3);
        WriteString(writer, "a");
        WriteString(writer, "a");
        WriteString(writer, "b");
        writer.WriteInt32(1);
        writer.WriteInt32(2);
        writer.WriteInt32(3);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var map = reader.ReadValue(ref pr);

        Assert.Equal(3, map.Count);
        Assert.Equal(new KeyValuePair<string, int>("a", 1), map[0]);
        Assert.Equal(new KeyValuePair<string, int>("a", 2), map[1]);
        Assert.Equal(new KeyValuePair<string, int>("b", 3), map[2]);
        Assert.True(map.HasDuplicateKeys);
    }

    [Fact]
    public void ReadValue_Empty_ReturnsEmptyMap()
    {
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var map = reader.ReadValue(ref pr);

        Assert.Equal(0, map.Count);
        Assert.False(map.HasDuplicateKeys);
    }

    [Fact]
    public void ReadTypedColumn_MixedRowSizes_PreservesPerRowEntries()
    {
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // 3 rows: row0 has 0 entries, row1 has 2 entries incl. a duplicate,
        // row2 has 1 entry. Cumulative offsets: 0, 2, 3.
        writer.WriteUInt64(0);
        writer.WriteUInt64(2);
        writer.WriteUInt64(3);
        // Flat keys (3 strings):
        WriteString(writer, "k");
        WriteString(writer, "k");
        WriteString(writer, "z");
        // Flat values (3 ints):
        writer.WriteInt32(10);
        writer.WriteInt32(20);
        writer.WriteInt32(99);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, column.Count);
        Assert.Equal(0, column[0].Count);
        Assert.Equal(2, column[1].Count);
        Assert.Equal(new KeyValuePair<string, int>("k", 10), column[1][0]);
        Assert.Equal(new KeyValuePair<string, int>("k", 20), column[1][1]);
        Assert.True(column[1].HasDuplicateKeys);
        Assert.Equal(1, column[2].Count);
        Assert.Equal(new KeyValuePair<string, int>("z", 99), column[2][0]);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_NoBytesConsumed()
    {
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = reader.ReadTypedColumn(ref pr, 0);

        Assert.Equal(0, column.Count);
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void ReadTypedColumn_AllEmptyRows_ReturnsEmptyMaps()
    {
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // 3 rows, all offsets 0.
        writer.WriteUInt64(0);
        writer.WriteUInt64(0);
        writer.WriteUInt64(0);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, column.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(0, column[i].Count);
        }
    }

    [Fact]
    public void ReadTypedColumn_NonMonotonicOffsets_ThrowsWithRowAndOffsets()
    {
        // Wire integrity: cumulative offsets must be non-decreasing. If row N's
        // offset is less than row (N-1)'s, the derived per-row count goes negative
        // — surface it as a typed error rather than letting the reader produce a
        // silently-corrupted column.
        var reader = CreateReader();

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // 3 rows with offsets [5, 2, 6]: row 1 goes backwards (5 → 2).
        // Last offset (6) drives totalEntries so we still have 6 keys+values to
        // consume before the per-row loop tries to bisect the array.
        writer.WriteUInt64(5);
        writer.WriteUInt64(2);
        writer.WriteUInt64(6);
        for (int i = 0; i < 6; i++) WriteString(writer, $"k{i}");
        for (int i = 0; i < 6; i++) writer.WriteInt32(i);

        var bytes = buffer.WrittenMemory;
        InvalidOperationException? captured = null;
        try
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            using var _ = reader.ReadTypedColumn(ref pr, 3);
        }
        catch (InvalidOperationException ex)
        {
            captured = ex;
        }

        Assert.NotNull(captured);
        Assert.Contains("Map row 1", captured!.Message);
        Assert.Contains("non-monotonic", captured.Message);
    }
}

/// <summary>
/// Regression test pinning the deliberate-but-lossy last-wins behaviour of
/// <see cref="MapColumnReader{TKey, TValue}"/> when duplicate keys are present
/// on the wire. The newer <see cref="MapEntriesColumnReader{TKey, TValue}"/> is
/// the lossless alternative; this test prevents a future refactor from silently
/// changing the existing semantics.
/// </summary>
public class MapColumnReaderDuplicateKeyTests
{
    [Fact]
    public void ReadValue_WithDuplicateKeys_IsLastWins()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var reader = (IColumnReader<Dictionary<string, int>>)factory.CreateReader("Map(String, Int32)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3);
        writer.WriteString("a");
        writer.WriteString("a");
        writer.WriteString("b");
        writer.WriteInt32(1);
        writer.WriteInt32(2);
        writer.WriteInt32(3);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var dict = reader.ReadValue(ref pr);

        Assert.Equal(2, dict.Count);
        Assert.Equal(2, dict["a"]); // last-wins
        Assert.Equal(3, dict["b"]);
    }
}

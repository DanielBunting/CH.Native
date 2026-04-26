using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Dynamic;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class DynamicColumnReaderTests
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);
    private static readonly ColumnWriterFactory WriterFactory = new(ColumnWriterRegistry.Default);
    private static readonly ColumnSkipperFactory SkipperFactory = new(ColumnSkipperRegistry.Default);

    private static ClickHouseDynamic Row(ITypedColumn col, int index)
        => (ClickHouseDynamic)col.GetValue(index)!;

    [Fact]
    public void ReadTypedColumn_SingleArmBlock_ReadsInt64Values()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3);          // structure version = FLATTENED
        writer.WriteVarInt(1ul);        // numberOfTypes
        writer.WriteString("Int64");

        // Indexes column — totalIndexValues = 2, so UInt8. All three rows select arm 0.
        writer.WriteByte(0);
        writer.WriteByte(0);
        writer.WriteByte(0);

        // Arm 0 (Int64) data.
        writer.WriteInt64(10);
        writer.WriteInt64(20);
        writer.WriteInt64(30);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, col.Count);
        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(10L, Row(col, 0).Value);
        Assert.Equal(20L, Row(col, 1).Value);
        Assert.Equal(30L, Row(col, 2).Value);
    }

    [Fact]
    public void ReadTypedColumn_MixedArmsAndNull_ReadsCorrectly()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3);
        writer.WriteVarInt(2ul);
        writer.WriteString("Int64");
        writer.WriteString("String");

        // rows: Int64, String, NULL (index == numberOfTypes = 2), Int64
        writer.WriteByte(0);
        writer.WriteByte(1);
        writer.WriteByte(2);
        writer.WriteByte(0);

        writer.WriteInt64(100);
        writer.WriteInt64(200);
        writer.WriteString("hi");

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);
        using var col = reader.ReadTypedColumn(ref pr, 4);

        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(100L, Row(col, 0).Value);
        Assert.Equal("String", Row(col, 1).DeclaredTypeName);
        Assert.Equal("hi", Row(col, 1).Value);
        Assert.True(Row(col, 2).IsNull);
        Assert.Equal(200L, Row(col, 3).Value);
    }

    [Fact]
    public void ReadPrefix_RejectsNonFlattenedStructureVersion()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1); // V1 — no longer supported

        NotSupportedException? thrown = null;
        try
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            reader.ReadPrefix(ref pr);
        }
        catch (NotSupportedException ex) { thrown = ex; }

        Assert.NotNull(thrown);
    }

    [Fact]
    public void RoundTrip_HeterogeneousRows_PreservesData()
    {
        var w = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic");
        var r = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        var source = new[]
        {
            new ClickHouseDynamic(0, 10L, "Int64"),
            new ClickHouseDynamic(0, "hello", "String"),
            ClickHouseDynamic.Null,
            new ClickHouseDynamic(0, 20L, "Int64"),
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WritePrefix(ref pw);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        r.ReadPrefix(ref pr);
        using var col = r.ReadTypedColumn(ref pr, source.Length);

        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(10L, Row(col, 0).Value);
        Assert.Equal("String", Row(col, 1).DeclaredTypeName);
        Assert.Equal("hello", Row(col, 1).Value);
        Assert.True(Row(col, 2).IsNull);
        Assert.Equal("Int64", Row(col, 3).DeclaredTypeName);
        Assert.Equal(20L, Row(col, 3).Value);
    }

    [Fact]
    public void WriteColumn_ThrowsWhenTypesExceedMaxTypes()
    {
        var w = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic(max_types=1)");

        var source = new[]
        {
            new ClickHouseDynamic(0, 1L, "Int64"),
            new ClickHouseDynamic(0, "x", "String"),
        };

        ArgumentException? thrown = null;
        try
        {
            using var buffer = new PooledBufferWriter();
            var pw = new ProtocolWriter(buffer);
            w.WritePrefix(ref pw);
            w.WriteColumn(ref pw, source);
        }
        catch (ArgumentException ex) { thrown = ex; }

        Assert.NotNull(thrown);
    }

    [Fact]
    public void Skipper_RoundTrip_ConsumesAllBytes()
    {
        var w = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic");
        var s = SkipperFactory.CreateSkipper("Dynamic");

        var source = new[]
        {
            new ClickHouseDynamic(0, 1L, "Int64"),
            new ClickHouseDynamic(0, "x", "String"),
            ClickHouseDynamic.Null,
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WritePrefix(ref pw);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.True(s.TrySkipColumn(ref pr, source.Length));
        Assert.Equal(0, pr.Remaining);
    }

    // Audit finding #26: A VarInt for `numberOfTypes` that exceeds Int32.MaxValue
    // currently throws raw OverflowException from `checked((int)...)`. The fix
    // should map this to a typed protocol exception (e.g. ClickHouseException
    // or a ClickHouseProtocolException subclass) so the caller can recognise
    // it and tear the connection down. These tests FAIL today because raw
    // OverflowException is what propagates.
    [Fact]
    public void ReadTypedColumn_NumberOfTypesExceedsInt32MaxValue_ThrowsTypedProtocolException()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3);
        // VarInt for (Int32.MaxValue + 1) = 2147483648 — within VarInt range
        // but not representable as int.
        writer.WriteVarInt((ulong)int.MaxValue + 1);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);

        Exception? thrown = null;
        try { _ = reader.ReadTypedColumn(ref pr, 1); }
        catch (Exception ex) { thrown = ex; }

        Assert.NotNull(thrown);
        // Desired post-fix: a ClickHouseException (or subclass) so the
        // connection-tear-down catch sites can recognise it. Raw
        // OverflowException slipping through is exactly the bug.
        Assert.IsAssignableFrom<ClickHouseException>(thrown);
    }

    [Fact]
    public void ReadTypedColumn_NumberOfTypesAtUInt64Max_ThrowsTypedProtocolException()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3);
        writer.WriteVarInt(ulong.MaxValue);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        reader.ReadPrefix(ref pr);

        Exception? thrown = null;
        try { _ = reader.ReadTypedColumn(ref pr, 1); }
        catch (Exception ex) { thrown = ex; }

        Assert.NotNull(thrown);
        Assert.IsAssignableFrom<ClickHouseException>(thrown);
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Data.Dynamic;
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
        writer.WriteUInt64(1);      // structure version
        writer.WriteUInt64(32);     // max_types
        writer.WriteUInt64(1);      // numberOfTypes
        writer.WriteString("Int64");

        // Variant section
        writer.WriteUInt64(0);      // discriminator version
        writer.WriteByte(0);
        writer.WriteByte(0);
        writer.WriteByte(0);
        writer.WriteInt64(10);
        writer.WriteInt64(20);
        writer.WriteInt64(30);
        // Shared arm — zero rows.

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
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
        writer.WriteUInt64(1);
        writer.WriteUInt64(32);
        writer.WriteUInt64(2);
        writer.WriteString("Int64");
        writer.WriteString("String");

        writer.WriteUInt64(0);
        // rows: Int64, String, NULL, Int64
        writer.WriteByte(0);
        writer.WriteByte(1);
        writer.WriteByte(255);
        writer.WriteByte(0);

        writer.WriteInt64(100);
        writer.WriteInt64(200);
        writer.WriteString("hi");
        // shared arm zero rows

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 4);

        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(100L, Row(col, 0).Value);
        Assert.Equal("String", Row(col, 1).DeclaredTypeName);
        Assert.Equal("hi", Row(col, 1).Value);
        Assert.True(Row(col, 2).IsNull);
        Assert.Equal(200L, Row(col, 3).Value);
    }

    [Fact]
    public void ReadTypedColumn_SharedArm_ReadsTypeNameAndValuePerRow()
    {
        var reader = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic(max_types=1)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1);
        writer.WriteUInt64(1);      // max_types=1
        writer.WriteUInt64(1);      // numberOfTypes=1 (only Int64 in declared arms)
        writer.WriteString("Int64");

        writer.WriteUInt64(0);
        // rows: declared Int64, shared String, shared Int32
        writer.WriteByte(0);
        writer.WriteByte(1);
        writer.WriteByte(1);

        writer.WriteInt64(7);       // declared arm 0 — 1 row
        // shared arm — 2 rows of (String type_name, binary value)
        writer.WriteString("String");
        writer.WriteString("overflow");
        writer.WriteString("Int32");
        writer.WriteInt32(-42);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(7L, Row(col, 0).Value);
        Assert.Equal("String", Row(col, 1).DeclaredTypeName);
        Assert.Equal("overflow", Row(col, 1).Value);
        Assert.Equal("Int32", Row(col, 2).DeclaredTypeName);
        Assert.Equal(-42, Row(col, 2).Value);
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
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
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
    public void RoundTrip_MaxTypesOne_OverflowRoutesThroughSharedArm()
    {
        var w = (DynamicColumnWriter)WriterFactory.CreateWriter("Dynamic(max_types=1)");
        var r = (DynamicColumnReader)ReaderFactory.CreateReader("Dynamic(max_types=1)");

        var source = new[]
        {
            new ClickHouseDynamic(0, 1L, "Int64"),
            new ClickHouseDynamic(0, "x", "String"),
            new ClickHouseDynamic(0, 2L, "Int64"),
        };

        using var buffer = new PooledBufferWriter();
        var pw = new ProtocolWriter(buffer);
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = r.ReadTypedColumn(ref pr, source.Length);

        Assert.Equal("Int64", Row(col, 0).DeclaredTypeName);
        Assert.Equal(1L, Row(col, 0).Value);
        Assert.Equal("String", Row(col, 1).DeclaredTypeName);
        Assert.Equal("x", Row(col, 1).Value);
        Assert.Equal("Int64", Row(col, 2).DeclaredTypeName);
        Assert.Equal(2L, Row(col, 2).Value);
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
        w.WriteColumn(ref pw, source);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        Assert.True(s.TrySkipColumn(ref pr, source.Length));
        Assert.Equal(0, pr.Remaining);
    }
}

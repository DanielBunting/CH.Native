using System.Buffers;
using System.Text.Json;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.Json;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.Json;

/// <summary>
/// Unit tests for the version-0 binary JSON decoder path. These validate the decoder's
/// handling of the flat typed-path table against hand-crafted bytes that match this
/// implementation's interpretation of the format.
/// </summary>
public class JsonBinaryDecoderTests
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);

    [Fact]
    public void DecodeVersion0_SingleIntPath_ProducesObject()
    {
        // Wire layout (v0 with already-consumed UInt64 version):
        //   UInt64 pathCount = 1
        //   String "age"
        //   String "Int64"
        //   Int64 × 2 values [10, 20]
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1);
        writer.WriteString("age");
        writer.WriteString("Int64");
        writer.WriteInt64(10);
        writer.WriteInt64(20);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var docs = JsonBinaryDecoder.DecodeVersion0(ref pr, rowCount: 2, ReaderFactory);

        Assert.Equal(2, docs.Length);
        Assert.Equal(10, docs[0].RootElement.GetProperty("age").GetInt64());
        Assert.Equal(20, docs[1].RootElement.GetProperty("age").GetInt64());
        foreach (var doc in docs) doc.Dispose();
    }

    [Fact]
    public void DecodeVersion0_TwoPaths_ProducesCombinedObject()
    {
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(2);                // pathCount
        writer.WriteString("name");
        writer.WriteString("score");
        writer.WriteString("String");
        writer.WriteString("Int32");
        // name column (String × 2)
        writer.WriteString("alice");
        writer.WriteString("bob");
        // score column (Int32 × 2)
        writer.WriteInt32(100);
        writer.WriteInt32(200);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var docs = JsonBinaryDecoder.DecodeVersion0(ref pr, rowCount: 2, ReaderFactory);

        Assert.Equal("alice", docs[0].RootElement.GetProperty("name").GetString());
        Assert.Equal(100, docs[0].RootElement.GetProperty("score").GetInt32());
        Assert.Equal("bob", docs[1].RootElement.GetProperty("name").GetString());
        Assert.Equal(200, docs[1].RootElement.GetProperty("score").GetInt32());
        foreach (var doc in docs) doc.Dispose();
    }

    [Fact]
    public void DecodeVersion0_DottedPath_ExpandsToNestedObject()
    {
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1);
        writer.WriteString("user.id");
        writer.WriteString("Int64");
        writer.WriteInt64(42);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var docs = JsonBinaryDecoder.DecodeVersion0(ref pr, rowCount: 1, ReaderFactory);

        Assert.Single(docs);
        var user = docs[0].RootElement.GetProperty("user");
        Assert.Equal(42, user.GetProperty("id").GetInt64());
        docs[0].Dispose();
    }

    [Fact]
    public void JsonColumnReader_V1StringPath_StillWorks()
    {
        // Regression — ensure the v1 (string) path still uses the existing code.
        var json = @"{""a"":1}";
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(1); // version 1
        writer.WriteString(json);

        var columnReader = new JsonColumnReader();
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        columnReader.ReadPrefix(ref pr);
        using var result = columnReader.ReadTypedColumn(ref pr, 1);

        Assert.Equal(1, result.Count);
        Assert.Equal(1, result[0].RootElement.GetProperty("a").GetInt32());
    }

    [Fact]
    public void JsonColumnReader_V0WithoutFactory_ThrowsExplanatory()
    {
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(0); // version 0 — binary, but no factory attached

        var columnReader = new JsonColumnReader(); // no factory
        var ex = Assert.Throws<NotSupportedException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            columnReader.ReadPrefix(ref pr);
            using var _ = columnReader.ReadTypedColumn(ref pr, 1);
        });
        Assert.Contains("ColumnReaderFactory", ex.Message);
    }

    [Fact]
    public void JsonColumnReader_V3_ThrowsExperimentalNotice()
    {
        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(3); // version 3

        var columnReader = new JsonColumnReader(ReaderFactory);
        var ex = Assert.Throws<NotSupportedException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            columnReader.ReadPrefix(ref pr);
            using var _ = columnReader.ReadTypedColumn(ref pr, 1);
        });
        Assert.Contains("version 3", ex.Message);
    }

    [Fact]
    public void DecodeVersion0_FixedWidthScalars_CoverValueArms()
    {
        var paths = new[] { "i8", "u8", "i16", "u16", "u32", "u64", "f32", "f64", "flag" };
        var types = new[] { "Int8", "UInt8", "Int16", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Bool" };

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64((ulong)paths.Length);
        foreach (var p in paths) writer.WriteString(p);
        foreach (var t in types) writer.WriteString(t);
        writer.WriteByte(5);              // Int8 -> sbyte
        writer.WriteByte(200);            // UInt8 -> byte
        writer.WriteInt16(-100);          // Int16 -> short
        writer.WriteUInt16(60000);        // UInt16 -> ushort
        writer.WriteUInt32(4000000000u);  // UInt32 -> uint
        writer.WriteUInt64(5000000000UL); // UInt64 -> ulong
        writer.WriteFloat32(1.5f);        // Float32 -> float
        writer.WriteFloat64(2.5);         // Float64 -> double
        writer.WriteByte(1);              // Bool -> true

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var docs = JsonBinaryDecoder.DecodeVersion0(ref pr, rowCount: 1, ReaderFactory);

        var root = docs[0].RootElement;
        Assert.Equal(5, root.GetProperty("i8").GetInt32());
        Assert.Equal(200, root.GetProperty("u8").GetInt32());
        Assert.Equal(-100, root.GetProperty("i16").GetInt32());
        Assert.Equal(60000, root.GetProperty("u16").GetInt32());
        Assert.Equal(4000000000u, root.GetProperty("u32").GetUInt32());
        Assert.Equal(5000000000UL, root.GetProperty("u64").GetUInt64());
        Assert.Equal(1.5f, root.GetProperty("f32").GetSingle());
        Assert.Equal(2.5, root.GetProperty("f64").GetDouble());
        Assert.True(root.GetProperty("flag").GetBoolean());
        docs[0].Dispose();
    }
}

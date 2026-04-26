using System.Buffers;
using System.Text.Json;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

public class JsonColumnSkipperTests
{
    private static ColumnSkipperFactory SkipperFactory() => new(ColumnSkipperRegistry.Default);

    [Fact]
    public void Skip_Version1_StringPerRow()
    {
        // JsonColumnWriter emits version 1 (string serialization).
        var docs = new[]
        {
            JsonDocument.Parse("{\"a\":1}"),
            JsonDocument.Parse("[1,2,3]"),
            JsonDocument.Parse("\"hello\""),
            JsonDocument.Parse("null"),
            JsonDocument.Parse("{}"),
        };

        var writer = new JsonColumnWriter();
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, docs);
        });

        var skipper = new JsonColumnSkipper(SkipperFactory());
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, docs.Length));
        Assert.Equal(0, reader.Remaining);

        foreach (var d in docs) d.Dispose();
    }

    [Fact]
    public void Skip_Version0_TypedPaths_HappyPath()
    {
        // Hand-craft the legacy version 0 binary format. There is no writer for it.
        // 2 paths: "ints" Int32, "names" String. 3 rows.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0);                  // version 0
            w.WriteUInt64(2);                  // path count (UInt64 in this format)
            w.WriteString("ints");             // path 0 name
            w.WriteString("names");            // path 1 name
            w.WriteString("Int32");            // path 0 type
            w.WriteString("String");           // path 1 type
            // Inner column data, in path order. rowCount = 3.
            w.WriteInt32(1); w.WriteInt32(2); w.WriteInt32(3);
            w.WriteString("a"); w.WriteString("bb"); w.WriteString("ccc");
        });

        var skipper = new JsonColumnSkipper(SkipperFactory());
        var reader = new ProtocolReader(seq);
        Assert.True(skipper.TrySkipColumn(ref reader, rowCount: 3));
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Skip_Version0_TruncatedPathCount_ReturnsFalse()
    {
        // Version byte present, but path count UInt64 truncated to 4 bytes.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0);
            w.WriteBytes(new byte[4]);
        });

        var skipper = new JsonColumnSkipper(SkipperFactory());
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 3));
    }

    [Fact]
    public void Skip_Version3_ThrowsNotSupported()
    {
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteUInt64(3));

        var skipper = new JsonColumnSkipper(SkipperFactory());

        NotSupportedException? caught = null;
        try
        {
            var reader = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref reader, rowCount: 1);
        }
        catch (NotSupportedException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void Skip_Version0_NoFactory_ThrowsNotSupported()
    {
        // Parameterless ctor → no factory → version 0 falls through to NotSupportedException.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) => w.WriteUInt64(0));

        var skipper = new JsonColumnSkipper();

        NotSupportedException? caught = null;
        try
        {
            var reader = new ProtocolReader(seq);
            skipper.TrySkipColumn(ref reader, rowCount: 1);
        }
        catch (NotSupportedException ex) { caught = ex; }
        Assert.NotNull(caught);
    }

    [Fact]
    public void Skip_Version0_UnknownTypeName_ReturnsFalse()
    {
        // Factory throws on unknown type → caught in skipper → returns false.
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            w.WriteUInt64(0);
            w.WriteUInt64(1);
            w.WriteString("bad");
            w.WriteString("ThisIsNotAType");
        });

        var skipper = new JsonColumnSkipper(SkipperFactory());
        var reader = new ProtocolReader(seq);
        Assert.False(skipper.TrySkipColumn(ref reader, rowCount: 1));
    }

    [Fact]
    public void ReaderVsSkipper_Version1_ConsumeSameBytes()
    {
        var docs = new[]
        {
            JsonDocument.Parse("{\"x\":1}"),
            JsonDocument.Parse("[true,false]"),
            JsonDocument.Parse("\"abc\""),
        };

        var writer = new JsonColumnWriter();
        var seq = SkipperTestBase.Encode((ref ProtocolWriter w) =>
        {
            writer.WritePrefix(ref w);
            writer.WriteColumn(ref w, docs);
        });

        var jsonReader = new JsonColumnReader();

        SkipperTestBase.AssertParity(
            seq, docs.Length,
            (ref ProtocolReader r) => jsonReader.ReadPrefix(ref r),
            (ref ProtocolReader r, int rc) =>
            {
                using var col = jsonReader.ReadTypedColumn(ref r, rc);
            },
            (ref ProtocolReader r, int rc) => new JsonColumnSkipper(SkipperFactory()).TrySkipColumn(ref r, rc));

        foreach (var d in docs) d.Dispose();
    }
}

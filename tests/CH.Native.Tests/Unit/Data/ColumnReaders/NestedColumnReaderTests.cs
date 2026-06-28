using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// A Nested(field1 T1, field2 T2) column is parallel arrays that share ONE offsets
/// block (sent once), then each field's element values flattened — verified against
/// the server. The reader holds the field ELEMENT readers, reads the shared offsets
/// once, and slices every field's flat values into row-major <c>object[][]</c>.
/// </summary>
public class NestedColumnReaderTests
{
    private static IColumnReader[] ElementReaders() => new IColumnReader[]
    {
        new Int32ColumnReader(),
        new StringColumnReader(),
    };

    [Fact]
    public void Constructor_RejectsEmptyFieldList()
    {
        Assert.Throws<ArgumentException>(() =>
            new NestedColumnReader(Array.Empty<IColumnReader>(), Array.Empty<string>()));
    }

    [Fact]
    public void Constructor_RejectsMismatchedNamesCount()
    {
        var fields = new IColumnReader[] { new Int32ColumnReader() };
        var names = new[] { "a", "b" };
        Assert.Throws<ArgumentException>(() => new NestedColumnReader(fields, names));
    }

    [Fact]
    public void TypeName_RendersFieldElementTypes()
    {
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });
        Assert.Equal("Nested(id Int32, name String)", sut.TypeName);
    }

    [Fact]
    public void FieldCount_AndFieldNames_ReflectConstructor()
    {
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });

        Assert.Equal(2, sut.FieldCount);
        Assert.Equal(new[] { "id", "name" }, sut.FieldNames);
    }

    [Fact]
    public void GetFieldIndex_ReturnsIndexOrMinusOne()
    {
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });

        Assert.Equal(0, sut.GetFieldIndex("id"));
        Assert.Equal(1, sut.GetFieldIndex("name"));
        Assert.Equal(-1, sut.GetFieldIndex("missing"));
        // Documented as case-sensitive (uses Ordinal). Pin that.
        Assert.Equal(-1, sut.GetFieldIndex("ID"));
    }

    [Fact]
    public void ReadTypedColumn_SharedOffsets_TransposesToRowMajor()
    {
        // Build a Nested(id Int32, name String) column with two rows via the writer
        // (shared offsets, once), then read it back:
        //   row 0: id=[1, 2], name=["a", "b"]
        //   row 1: id=[3],    name=["c"]
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        var nestedWriter = new NestedColumnWriter(
            new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() },
            new[] { "id", "name" });
        nestedWriter.WriteColumn(ref writer, new[]
        {
            new object[] { new[] { 1, 2 }, new[] { "a", "b" } },
            new object[] { new[] { 3 }, new[] { "c" } },
        });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });

        using var column = sut.ReadTypedColumn(ref reader, 2);

        Assert.Equal(2, column.Count);
        Assert.Equal(new[] { 1, 2 }, (int[])column[0][0]);
        Assert.Equal(new[] { "a", "b" }, (string[])column[0][1]);
        Assert.Equal(new[] { 3 }, (int[])column[1][0]);
        Assert.Equal(new[] { "c" }, (string[])column[1][1]);
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_ReturnsEmptyWithoutReadingBytes()
    {
        var sut = new NestedColumnReader(
            new IColumnReader[] { new Int32ColumnReader() },
            new[] { "id" });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        using var column = sut.ReadTypedColumn(ref reader, 0);

        Assert.Equal(0, column.Count);
    }
}

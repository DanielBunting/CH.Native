using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Exceptions;
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

    [Fact]
    public void ClrType_IsObjectArray()
    {
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });
        Assert.Equal(typeof(object[]), sut.ClrType);
    }

    [Fact]
    public void ReadValue_SingleRow_RoundTripsViaWriterWriteValue()
    {
        // ReadValue (a single Nested value: its offset, then the row's field elements)
        // is the path used when a Nested appears as an element of another composite.
        // Round-trip it against NestedColumnWriter.WriteValue.
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var nestedWriter = new NestedColumnWriter(
            new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() },
            new[] { "id", "name" });
        nestedWriter.WriteValue(ref writer, new object[] { new[] { 1, 2, 3 }, new[] { "a", "b", "c" } });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });
        var row = sut.ReadValue(ref reader);

        Assert.Equal(new[] { 1, 2, 3 }, (int[])row[0]);
        Assert.Equal(new[] { "a", "b", "c" }, (string[])row[1]);
    }

    [Fact]
    public void ReadValue_EmptyRow_RoundTrips()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        var nestedWriter = new NestedColumnWriter(
            new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() },
            new[] { "id", "name" });
        nestedWriter.WriteValue(ref writer, new object[] { Array.Empty<int>(), Array.Empty<string>() });

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var sut = new NestedColumnReader(ElementReaders(), new[] { "id", "name" });
        var row = sut.ReadValue(ref reader);

        Assert.Empty((int[])row[0]);
        Assert.Empty((string[])row[1]);
    }

    [Fact]
    public void ReadTypedColumn_DecreasingOffset_ThrowsProtocolException()
    {
        // The cumulative offsets must be monotonically non-decreasing; a row whose
        // offset drops below the previous cumulative is a malformed/garbage stream and
        // must fail loudly rather than slice a negative-length span.
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        pw.WriteUInt64(5); // row 0 cumulative offset
        pw.WriteUInt64(2); // row 1 cumulative offset < previous → invalid

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var sut = new NestedColumnReader(new IColumnReader[] { new Int32ColumnReader() }, new[] { "id" });

        ClickHouseProtocolException? caught = null;
        try
        {
            using var _ = sut.ReadTypedColumn(ref reader, 2);
        }
        catch (ClickHouseProtocolException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Contains("row 1", caught!.Message);
        Assert.Contains("non-decreasing", caught.Message);
    }
}

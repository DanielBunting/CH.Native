using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// LazyNullableStringColumnReader is the lazy-mode path for Nullable(String).
/// Wire layout: bitmap byte per row, then ALL string values (placeholder
/// strings for null slots). The non-generic ReadTypedColumn returns a
/// <see cref="NullableRawStringColumn"/> that defers UTF-8 decoding to
/// <c>GetValue</c>; the generic ReadTypedColumn eagerly materialises strings.
/// </summary>
public class LazyNullableStringColumnReaderTests
{
    [Fact]
    public void TypeName_IsNullableString()
    {
        var sut = new LazyNullableStringColumnReader(new StringColumnReader(lazy: true));
        Assert.Equal("Nullable(String)", sut.TypeName);
    }

    [Fact]
    public void ClrType_IsString()
    {
        var sut = new LazyNullableStringColumnReader(new StringColumnReader(lazy: true));
        Assert.Equal(typeof(string), sut.ClrType);
    }

    private static byte[] BuildWire(byte[] bitmap, string[] values)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        foreach (var b in bitmap) writer.WriteByte(b);
        foreach (var v in values) writer.WriteString(v);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void GenericReadTypedColumn_AppliesBitmapToValues()
    {
        var bytes = BuildWire(
            new byte[] { 0, 1, 0 },
            new[] { "first", "<placeholder>", "third" });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        IColumnReader<string> sut = new LazyNullableStringColumnReader(new StringColumnReader());
        using var column = sut.ReadTypedColumn(ref reader, 3);

        Assert.Equal("first", column[0]);
        Assert.Null(column[1]);
        Assert.Equal("third", column[2]);
    }

    [Fact]
    public void GenericReadTypedColumn_ZeroRows_ReturnsEmpty()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        IColumnReader<string> sut = new LazyNullableStringColumnReader(new StringColumnReader());

        using var column = sut.ReadTypedColumn(ref reader, 0);
        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void NonGenericReadTypedColumn_ReturnsNullableRawStringColumn()
    {
        var bytes = BuildWire(
            new byte[] { 0, 1 },
            new[] { "hello", "<placeholder>" });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        IColumnReader sut = new LazyNullableStringColumnReader(new StringColumnReader());
        using var column = sut.ReadTypedColumn(ref reader, 2);

        Assert.IsType<NullableRawStringColumn>(column);
        Assert.Equal(2, column.Count);
        Assert.Equal("hello", column.GetValue(0));
        Assert.Null(column.GetValue(1));
    }

    [Fact]
    public void ReadValue_ConsumesBitmapByteAndValueEvenForNullSlot()
    {
        // The doc-comment on ReadValue is explicit: it always consumes the
        // value bytes (ClickHouse sends a placeholder for null rows). Pin
        // that — a regression that skipped the value read would desync the
        // pipe for subsequent rows.
        var bytes = BuildWire(new byte[] { 1 }, new[] { "<placeholder>" });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        IColumnReader<string> sut = new LazyNullableStringColumnReader(new StringColumnReader());
        var result = sut.ReadValue(ref reader);

        Assert.Null(result);
    }

    [Fact]
    public void ReadValue_NonNullSlot_ReturnsValue()
    {
        var bytes = BuildWire(new byte[] { 0 }, new[] { "value" });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        IColumnReader<string> sut = new LazyNullableStringColumnReader(new StringColumnReader());
        Assert.Equal("value", sut.ReadValue(ref reader));
    }
}

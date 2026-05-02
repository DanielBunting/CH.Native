using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Nullable wire format: a 1-byte-per-row null bitmap, then ALL values
/// (including placeholder values for null slots). The reader must apply the
/// bitmap as a post-pass over the materialised inner column.
/// </summary>
public class NullableColumnReaderTests
{
    [Fact]
    public void TypeName_NamespacesUnderlying()
    {
        var sut = new NullableColumnReader<int>(new Int32ColumnReader());
        Assert.Equal("Nullable(Int32)", sut.TypeName);
    }

    [Fact]
    public void ClrType_IsNullableUnderlying()
    {
        Assert.Equal(typeof(int?), new NullableColumnReader<int>(new Int32ColumnReader()).ClrType);
    }

    [Fact]
    public void ReadColumn_AllNullBitmap_ReturnsAllNulls()
    {
        // Bitmap: 3 ones. Then 3 placeholder int32 values (zero-filled).
        var wire = new byte[] { 0x01, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 3);

        Assert.Null(column[0]);
        Assert.Null(column[1]);
        Assert.Null(column[2]);
    }

    [Fact]
    public void ReadColumn_NoNullBitmap_ReturnsAllValues()
    {
        // Bitmap: 3 zeros. Values: 1, 2, 3 as little-endian int32.
        var wire = new byte[] { 0x00, 0x00, 0x00, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 };
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 3);

        Assert.Equal(1, column[0]);
        Assert.Equal(2, column[1]);
        Assert.Equal(3, column[2]);
    }

    [Fact]
    public void ReadColumn_MixedBitmap_AppliesPerSlot()
    {
        var wire = new byte[] { 0x00, 0x01, 0x00, 42, 0, 0, 0, 99, 0, 0, 0, 7, 0, 0, 0 };
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 3);

        Assert.Equal(42, column[0]);
        Assert.Null(column[1]);
        Assert.Equal(7, column[2]);
    }

    [Fact]
    public void ReadColumn_LargeBitmap_HandlesPoolPath()
    {
        // 300 rows forces the pool path (threshold is 256).
        const int n = 300;
        var values = new int?[n];
        for (int i = 0; i < n; i++) values[i] = (i % 3 == 0) ? null : i;

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        new NullableColumnWriter<int>(new Int32ColumnWriter()).WriteColumn(ref writer, values);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, n);

        for (int i = 0; i < n; i++) Assert.Equal(values[i], column[i]);
    }

    [Fact]
    public void ReadColumn_RowCountZero_ReturnsEmptyColumnWithoutReadingBytes()
    {
        var wire = Array.Empty<byte>();
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = new NullableColumnReader<int>(new Int32ColumnReader())
            .ReadTypedColumn(ref reader, 0);

        Assert.Equal(0, column.Count);
    }

    [Fact]
    public void ReadValue_HonoursPerRowBitmapByte()
    {
        var sut = new NullableColumnReader<int>(new Int32ColumnReader());

        var nullWire = new byte[] { 0x01, 0, 0, 0, 0 };
        var nullReader = new ProtocolReader(new ReadOnlySequence<byte>(nullWire));
        Assert.Null(sut.ReadValue(ref nullReader));

        var valueWire = new byte[] { 0x00, 42, 0, 0, 0 };
        var valueReader = new ProtocolReader(new ReadOnlySequence<byte>(valueWire));
        Assert.Equal(42, sut.ReadValue(ref valueReader));
    }
}

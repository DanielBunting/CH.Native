using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class NothingColumnReaderTests
{
    private const byte Sentinel = 0xAB;

    [Fact]
    public void TypeName_IsNothing()
    {
        Assert.Equal("Nothing", new NothingColumnReader().TypeName);
    }

    [Fact]
    public void ClrType_IsObject()
    {
        Assert.Equal(typeof(object), new NothingColumnReader().ClrType);
    }

    [Fact]
    public void ReadValue_ConsumesExactlyOneByte_ReturnsNull()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x00, Sentinel }));

        var value = new NothingColumnReader().ReadValue(ref reader);

        Assert.Null(value);
        Assert.Equal(Sentinel, reader.ReadByte());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(100)]
    public void ReadTypedColumn_ConsumesOneBytePerRow_AllValuesNull(int rowCount)
    {
        var bytes = new byte[rowCount + 1];
        bytes[rowCount] = Sentinel;
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new NothingColumnReader().ReadTypedColumn(ref reader, rowCount);

        Assert.Equal(rowCount, column.Count);
        for (int i = 0; i < rowCount; i++)
        {
            Assert.Null(((ITypedColumn)column).GetValue(i));
            Assert.True(((ITypedColumn)column).IsNull(i));
        }
        Assert.Equal(Sentinel, reader.ReadByte());
    }

    [Fact]
    public void ReadTypedColumn_ZeroRows_ConsumesNothing()
    {
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { Sentinel }));

        using var column = new NothingColumnReader().ReadTypedColumn(ref reader, 0);

        Assert.Equal(0, column.Count);
        Assert.Equal(Sentinel, reader.ReadByte());
    }

    // Block reading dispatches through the non-generic IColumnReader interface.
    [Fact]
    public void ReadTypedColumn_ThroughNonGenericInterface_AllValuesNull()
    {
        IColumnReader reader = new NothingColumnReader();
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x00, 0x00, Sentinel }));

        using var column = reader.ReadTypedColumn(ref protocolReader, 2);

        Assert.Equal(2, column.Count);
        Assert.Null(column.GetValue(0));
        Assert.Null(column.GetValue(1));
        Assert.Equal(Sentinel, protocolReader.ReadByte());
    }

    [Fact]
    public void Registry_ResolvesNothingReader()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Nothing");
        Assert.IsType<NothingColumnReader>(reader);
    }

    [Fact]
    public void Registry_LazyStrings_ResolvesNothingReader()
    {
        var reader = ColumnReaderRegistry.LazyStrings.GetReader("Nothing");
        Assert.IsType<NothingColumnReader>(reader);
    }

    // Bare `SELECT NULL` arrives as Nullable(Nothing): null bitmap (1 byte/row) followed
    // by the inner Nothing data (1 dummy byte/row).
    [Fact]
    public void NullableNothing_DecodesSelectNullWire()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Nullable(Nothing)");

        var wire = new byte[] { 0x01, 0x00, Sentinel };
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = reader.ReadTypedColumn(ref protocolReader, 1);

        Assert.Equal(1, column.Count);
        Assert.Null(column.GetValue(0));
        Assert.Equal(Sentinel, protocolReader.ReadByte());
    }

    [Fact]
    public void NullableNothing_MultiRow_AllNull()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Nullable(Nothing)");

        // 3 rows: bitmap [1,1,1] + 3 dummy bytes
        var wire = new byte[] { 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, Sentinel };
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = reader.ReadTypedColumn(ref protocolReader, 3);

        Assert.Equal(3, column.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Null(column.GetValue(i));
        }
        Assert.Equal(Sentinel, protocolReader.ReadByte());
    }

    // `SELECT []` arrives as Array(Nothing): UInt64 end-offset per row, then element
    // data for the flattened count (zero elements -> no data).
    [Fact]
    public void ArrayNothing_DecodesSelectEmptyArrayWire()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Array(Nothing)");

        var wire = new byte[9];
        wire[8] = Sentinel; // offsets: one UInt64 = 0, then sentinel
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = reader.ReadTypedColumn(ref protocolReader, 1);

        Assert.Equal(1, column.Count);
        var array = Assert.IsAssignableFrom<System.Collections.IEnumerable>(column.GetValue(0));
        Assert.Empty(array.Cast<object?>());
        Assert.Equal(Sentinel, protocolReader.ReadByte());
    }

    [Fact]
    public void ArrayNothing_NonEmpty_YieldsNulls()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Array(Nothing)");

        // 1 row with 2 elements: offset = 2, then 2 dummy bytes
        var wire = new byte[] { 0x02, 0, 0, 0, 0, 0, 0, 0, 0x00, 0x00, Sentinel };
        var protocolReader = new ProtocolReader(new ReadOnlySequence<byte>(wire));

        using var column = reader.ReadTypedColumn(ref protocolReader, 1);

        var array = Assert.IsAssignableFrom<System.Collections.IEnumerable>(column.GetValue(0))
            .Cast<object?>().ToList();
        Assert.Equal(2, array.Count);
        Assert.All(array, Assert.Null);
        Assert.Equal(Sentinel, protocolReader.ReadByte());
    }
}

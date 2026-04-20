using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

public class TimeColumnReaderTests
{
    [Fact]
    public void TypeName_IsTime()
    {
        Assert.Equal("Time", new TimeColumnReader().TypeName);
    }

    [Fact]
    public void ClrType_IsTimeOnly()
    {
        Assert.Equal(typeof(TimeOnly), new TimeColumnReader().ClrType);
    }

    [Theory]
    [InlineData(0, "00:00:00")]
    [InlineData(3661, "01:01:01")]
    [InlineData(86399, "23:59:59")]
    public void ReadValue_DecodesSecondsSinceMidnight(int seconds, string expected)
    {
        var bytes = new byte[4];
        BitConverter.TryWriteBytes(bytes, seconds);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var value = new TimeColumnReader().ReadValue(ref reader);

        Assert.Equal(TimeOnly.Parse(expected), value);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(86400)]
    [InlineData(int.MaxValue)]
    public void ReadValue_OutOfRange_Throws(int seconds)
    {
        var bytes = new byte[4];
        BitConverter.TryWriteBytes(bytes, seconds);
        var columnReader = new TimeColumnReader();

        Assert.Throws<OverflowException>(() => ReadOne(columnReader, bytes));

        static void ReadOne(TimeColumnReader r, byte[] data)
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(data));
            r.ReadValue(ref pr);
        }
    }

    [Fact]
    public void ReadTypedColumn_ReadsAllRows()
    {
        int[] secondsValues = [0, 3600, 7200, 86399];
        var bytes = new byte[secondsValues.Length * 4];
        for (int i = 0; i < secondsValues.Length; i++)
        {
            BitConverter.TryWriteBytes(bytes.AsSpan(i * 4), secondsValues[i]);
        }
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        using var column = new TimeColumnReader().ReadTypedColumn(ref reader, secondsValues.Length);

        Assert.Equal(4, column.Count);
        Assert.Equal(new TimeOnly(0, 0, 0), column[0]);
        Assert.Equal(new TimeOnly(1, 0, 0), column[1]);
        Assert.Equal(new TimeOnly(2, 0, 0), column[2]);
        Assert.Equal(new TimeOnly(23, 59, 59), column[3]);
    }

    [Fact]
    public void Registry_ResolvesTimeReader()
    {
        var reader = ColumnReaderRegistry.Default.GetReader("Time");
        Assert.IsType<TimeColumnReader>(reader);
    }
}

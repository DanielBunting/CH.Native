using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// <c>SimpleAggregateFunction(name, T)</c> is a wire-format pass-through for the inner
/// type <c>T</c>. The factories route through the inner type's reader / writer / skipper
/// directly — no wrapper classes. These tests prove the routing returns something with
/// the inner CLR type and round-trips byte-for-byte through the standard inner-type path.
/// </summary>
public class SimpleAggregateFunctionRoutingTests
{
    [Theory]
    [InlineData("SimpleAggregateFunction(sum, Int32)", typeof(int))]
    [InlineData("SimpleAggregateFunction(sum, Int64)", typeof(long))]
    [InlineData("SimpleAggregateFunction(any, String)", typeof(string))]
    [InlineData("SimpleAggregateFunction(max, Float64)", typeof(double))]
    public void ReaderFactory_RoutesToInnerClrType(string typeName, System.Type expectedClrType)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var reader = factory.CreateReader(typeName);
        Assert.Equal(expectedClrType, reader.ClrType);
    }

    [Theory]
    [InlineData("SimpleAggregateFunction(sum, Int32)", typeof(int))]
    [InlineData("SimpleAggregateFunction(any, String)", typeof(string))]
    public void WriterFactory_RoutesToInnerClrType(string typeName, System.Type expectedClrType)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var writer = factory.CreateWriter(typeName);
        Assert.Equal(expectedClrType, writer.ClrType);
    }

    [Fact]
    public void SkipperFactory_RoutesToInnerSkipper()
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var skipper = factory.CreateSkipper("SimpleAggregateFunction(sum, Int32)");
        Assert.NotNull(skipper);
    }

    [Fact]
    public void RoundTrip_Int32_Inner_PreservesValues()
    {
        var readerFactory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var writerFactory = new ColumnWriterFactory(ColumnWriterRegistry.Default);

        var reader = (IColumnReader<int>)readerFactory.CreateReader("SimpleAggregateFunction(sum, Int32)");
        var writer = (IColumnWriter<int>)writerFactory.CreateWriter("SimpleAggregateFunction(sum, Int32)");

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WriteColumn(ref pw, new[] { 1, 2, 3, -1, int.MaxValue });

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 5);

        Assert.Equal(5, col.Count);
        Assert.Equal(1, col[0]);
        Assert.Equal(2, col[1]);
        Assert.Equal(3, col[2]);
        Assert.Equal(-1, col[3]);
        Assert.Equal(int.MaxValue, col[4]);
    }

    [Fact]
    public void RoundTrip_Nullable_Int64_Inner_PreservesValues()
    {
        var readerFactory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var writerFactory = new ColumnWriterFactory(ColumnWriterRegistry.Default);

        var reader = (IColumnReader<long?>)readerFactory.CreateReader("SimpleAggregateFunction(sum, Nullable(Int64))");
        var writer = (IColumnWriter<long?>)writerFactory.CreateWriter("SimpleAggregateFunction(sum, Nullable(Int64))");

        var values = new long?[] { 1L, null, 100L, null, -7L };
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WriteColumn(ref pw, values);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, values.Length);

        for (int i = 0; i < values.Length; i++)
            Assert.Equal(values[i], col[i]);
    }

    [Fact]
    public void ReaderFactory_MissingTypeArg_ThrowsFormat()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        Assert.Throws<FormatException>(() => factory.CreateReader("SimpleAggregateFunction(sum)"));
    }

    [Fact]
    public void WriterFactory_MissingTypeArg_ThrowsFormat()
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var ex = Assert.Throws<FormatException>(
            () => factory.CreateWriter("SimpleAggregateFunction(sum)"));
        Assert.Contains("exactly one type argument", ex.Message);
    }

    [Fact]
    public void SkipperFactory_MissingTypeArg_ThrowsFormat()
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var ex = Assert.Throws<FormatException>(
            () => factory.CreateSkipper("SimpleAggregateFunction(sum)"));
        Assert.Contains("exactly one type argument", ex.Message);
    }

    [Fact]
    public void WriterFactory_TooManyTypeArgs_ThrowsFormat()
    {
        // Two type args is invalid for SimpleAggregateFunction. Note: the parser
        // accepts this shape syntactically; the validation lives at the factory.
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        Assert.Throws<FormatException>(
            () => factory.CreateWriter("SimpleAggregateFunction(sum, Int32, Int64)"));
    }
}

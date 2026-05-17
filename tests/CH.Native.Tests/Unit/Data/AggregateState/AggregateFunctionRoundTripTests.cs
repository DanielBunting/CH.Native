using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.AggregateState;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.AggregateState;

/// <summary>
/// Round-trip tests for <c>AggregateFunction</c> reader, writer, and skipper through
/// the factories. Each test writes synthesized state bytes, reads them back, and
/// asserts the bytes match. Covers the three tier-1 wire formats (Phase 0 findings):
/// fixed-size (sum), varuint (count), and flag+fixed (min/max/any/anyLast).
/// </summary>
public class AggregateFunctionRoundTripTests
{
    [Theory]
    [InlineData("AggregateFunction(sum, Int32)", 8)]
    [InlineData("AggregateFunction(sum, Int64)", 8)]
    [InlineData("AggregateFunction(sum, Float64)", 8)]
    [InlineData("AggregateFunction(sum, UInt256)", 32)]
    [InlineData("AggregateFunction(sum, Decimal128(6))", 16)]
    [InlineData("AggregateFunction(sum, Decimal256(8))", 32)]
    public void Sum_RoundTrips_FixedSize(string typeName, int expectedBytes)
    {
        var states = new[]
        {
            new ClickHouseAggregateState(MakeBytes(expectedBytes, 0x01), "sum"),
            new ClickHouseAggregateState(MakeBytes(expectedBytes, 0xFF), "sum"),
            new ClickHouseAggregateState(new byte[expectedBytes], "sum"),
        };

        var roundTripped = RoundTrip(typeName, states);

        for (int i = 0; i < states.Length; i++)
        {
            Assert.Equal(expectedBytes, roundTripped[i].State.Length);
            Assert.Equal(states[i].State, roundTripped[i].State);
            Assert.Equal("sum", roundTripped[i].FunctionName);
        }
    }

    [Theory]
    [InlineData(new byte[] { 0x01 })]                       // count = 1
    [InlineData(new byte[] { 0x7F })]                       // count = 127
    [InlineData(new byte[] { 0x80, 0x01 })]                 // count = 128
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x0F })] // count near 2^32
    public void Count_RoundTrips_VarUInt(byte[] stateBytes)
    {
        var states = new[] { new ClickHouseAggregateState(stateBytes, "count") };
        var roundTripped = RoundTrip("AggregateFunction(count)", states);
        Assert.Equal(stateBytes, roundTripped[0].State);
        Assert.Equal("count", roundTripped[0].FunctionName);
    }

    [Fact]
    public void Count_MultipleRows_Concatenated_VarUInt()
    {
        // Concatenated varints decode independently: 0x01 0x80 0x01 0x7F = (1, 128, 127)
        var states = new[]
        {
            new ClickHouseAggregateState(new byte[] { 0x01 }, "count"),
            new ClickHouseAggregateState(new byte[] { 0x80, 0x01 }, "count"),
            new ClickHouseAggregateState(new byte[] { 0x7F }, "count"),
        };
        var roundTripped = RoundTrip("AggregateFunction(count)", states);
        Assert.Equal(new byte[] { 0x01 }, roundTripped[0].State);
        Assert.Equal(new byte[] { 0x80, 0x01 }, roundTripped[1].State);
        Assert.Equal(new byte[] { 0x7F }, roundTripped[2].State);
    }

    [Theory]
    [InlineData("AggregateFunction(min, Int32)", 5)]
    [InlineData("AggregateFunction(max, Int32)", 5)]
    [InlineData("AggregateFunction(any, Int32)", 5)]
    [InlineData("AggregateFunction(anyLast, Int32)", 5)]
    [InlineData("AggregateFunction(min, Int64)", 9)]
    [InlineData("AggregateFunction(min, DateTime)", 5)]
    [InlineData("AggregateFunction(min, UUID)", 17)]
    [InlineData("AggregateFunction(min, Date)", 3)]
    public void MinMax_HasValue_RoundTrips_FlagPlusFixed(string typeName, int totalBytes)
    {
        // Flag = 0x01 (has value), followed by (totalBytes - 1) data bytes.
        var stateBytes = new byte[totalBytes];
        stateBytes[0] = 0x01;
        for (int i = 1; i < totalBytes; i++) stateBytes[i] = (byte)(i * 7);

        var states = new[] { new ClickHouseAggregateState(stateBytes, ExtractFunctionName(typeName)) };
        var roundTripped = RoundTrip(typeName, states);

        Assert.Equal(stateBytes, roundTripped[0].State);
    }

    [Theory]
    [InlineData("AggregateFunction(min, Int32)")]
    [InlineData("AggregateFunction(max, Int64)")]
    public void MinMax_EmptyGroup_RoundTrips_SingleZeroByte(string typeName)
    {
        // Empty-group state is just the 0x00 "no value" flag.
        var states = new[] { new ClickHouseAggregateState(new byte[] { 0x00 }, ExtractFunctionName(typeName)) };
        var roundTripped = RoundTrip(typeName, states);
        Assert.Equal(new byte[] { 0x00 }, roundTripped[0].State);
    }

    [Fact]
    public void Skipper_ConsumesSameBytes_AsReader_Sum_Int32()
    {
        const string typeName = "AggregateFunction(sum, Int32)";
        var states = new[]
        {
            new ClickHouseAggregateState(MakeBytes(8, 0x01), "sum"),
            new ClickHouseAggregateState(MakeBytes(8, 0x02), "sum"),
        };

        var bytes = WriteStates(typeName, states);

        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var beforeRead = reader.Remaining;
        new ColumnReaderFactory(ColumnReaderRegistry.Default)
            .CreateReader(typeName)
            .ReadTypedColumn(ref reader, states.Length);
        var consumedByRead = beforeRead - reader.Remaining;

        var skipReader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var beforeSkip = skipReader.Remaining;
        Assert.True(new ColumnSkipperFactory(ColumnSkipperRegistry.Default)
            .CreateSkipper(typeName)
            .TrySkipColumn(ref skipReader, states.Length));
        var consumedBySkip = beforeSkip - skipReader.Remaining;

        Assert.Equal(consumedByRead, consumedBySkip);
    }

    [Fact]
    public void Factory_UnsupportedFunction_ThrowsActionableNotSupported()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var ex = Assert.Throws<NotSupportedException>(
            () => factory.CreateReader("AggregateFunction(quantilesTDigest(0.5), Float64)"));

        Assert.Contains("quantilesTDigest", ex.Message);
        Assert.Contains("finalizeAggregation", ex.Message);
        Assert.Contains("hex(", ex.Message);
    }

    [Fact]
    public void Factory_NullableInner_ThrowsActionableNotSupported()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var ex = Assert.Throws<NotSupportedException>(
            () => factory.CreateReader("AggregateFunction(sum, Nullable(Int32))"));

        Assert.Contains("Nullable", ex.Message);
    }

    [Fact]
    public void Factory_Reader_ClrType_IsClickHouseAggregateState()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var reader = factory.CreateReader("AggregateFunction(sum, Int32)");
        Assert.Equal(typeof(ClickHouseAggregateState), reader.ClrType);
        Assert.Equal("AggregateFunction(sum, Int32)", reader.TypeName);
    }

    // --- Helpers ---

    private static ClickHouseAggregateState[] RoundTrip(string typeName, ClickHouseAggregateState[] states)
    {
        var bytes = WriteStates(typeName, states);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        using var column = ((IColumnReader<ClickHouseAggregateState>)
            new ColumnReaderFactory(ColumnReaderRegistry.Default).CreateReader(typeName))
            .ReadTypedColumn(ref reader, states.Length);
        var result = new ClickHouseAggregateState[column.Count];
        for (int i = 0; i < column.Count; i++) result[i] = column[i];
        return result;
    }

    private static ReadOnlyMemory<byte> WriteStates(string typeName, ClickHouseAggregateState[] states)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        ((IColumnWriter<ClickHouseAggregateState>)
            new ColumnWriterFactory(ColumnWriterRegistry.Default).CreateWriter(typeName))
            .WriteColumn(ref pw, states);
        return buffer.WrittenMemory;
    }

    private static byte[] MakeBytes(int length, byte fill)
    {
        var b = new byte[length];
        for (int i = 0; i < length; i++) b[i] = fill;
        return b;
    }

    private static string ExtractFunctionName(string typeName)
    {
        // "AggregateFunction(min, Int32)" -> "min"
        var start = typeName.IndexOf('(') + 1;
        var comma = typeName.IndexOf(',', start);
        var end = comma < 0 ? typeName.IndexOf(')', start) : comma;
        return typeName[start..end].Trim();
    }
}

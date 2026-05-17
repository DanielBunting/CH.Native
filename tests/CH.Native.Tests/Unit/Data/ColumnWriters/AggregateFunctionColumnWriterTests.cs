using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.AggregateState;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Direct unit coverage for <see cref="AggregateFunctionColumnWriter"/>. The
/// round-trip tests in <c>AggregateFunctionRoundTripTests</c> cover the typed
/// happy paths; this file targets the null guards, the boxed
/// <see cref="IColumnWriter"/> interface paths, and the metadata getters that
/// the higher-level tests don't reach.
/// </summary>
public class AggregateFunctionColumnWriterTests
{
    private const string TypeName = "AggregateFunction(sum, Int32)";

    private static AggregateFunctionColumnWriter NewSut() =>
        new(TypeName, new FixedSizeStateFormat(size: 8));

    private static ClickHouseAggregateState NewState(byte fill) =>
        new(new byte[] { fill, fill, fill, fill, fill, fill, fill, fill }, "sum");

    // --- Metadata ------------------------------------------------------------

    [Fact]
    public void TypeName_Getter_ReturnsConfiguredValue()
    {
        Assert.Equal(TypeName, NewSut().TypeName);
    }

    [Fact]
    public void ClrType_Getter_ReturnsClickHouseAggregateState()
    {
        Assert.Equal(typeof(ClickHouseAggregateState), NewSut().ClrType);
    }

    [Fact]
    public void NullPlaceholder_ReturnsEmpty()
    {
        // Empty is the documented sentinel for "no value" in the typed surface.
        Assert.Same(ClickHouseAggregateState.Empty, NewSut().NullPlaceholder);
    }

    // --- Typed WriteValue null guard ----------------------------------------

    [Fact]
    public void WriteValue_NullState_ThrowsArgumentNull()
    {
        var sut = NewSut();
        var buffer = new ArrayBufferWriter<byte>();

        Assert.Throws<ArgumentNullException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            sut.WriteValue(ref pw, null!);
        });
    }

    // --- Non-generic IColumnWriter.WriteValue --------------------------------

    [Fact]
    public void NonGenericWriteValue_NullValue_ThrowsArgumentNull()
    {
        IColumnWriter sut = NewSut();
        var buffer = new ArrayBufferWriter<byte>();

        Assert.Throws<ArgumentNullException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            sut.WriteValue(ref pw, value: null);
        });
    }

    [Fact]
    public void NonGenericWriteValue_TypedValue_WritesExpectedBytes()
    {
        IColumnWriter sut = NewSut();
        var state = NewState(fill: 0x01);

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        sut.WriteValue(ref pw, state);

        Assert.Equal(state.State, buffer.WrittenMemory.ToArray());
    }

    // --- Non-generic IColumnWriter.WriteColumn -------------------------------

    [Fact]
    public void NonGenericWriteColumn_HappyPath_WritesAllRowsConcatenated()
    {
        IColumnWriter sut = NewSut();
        var state1 = NewState(fill: 0x01);
        var state2 = NewState(fill: 0x02);

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        sut.WriteColumn(ref pw, new object?[] { state1, state2 });

        var expected = new byte[16];
        Array.Fill(expected, (byte)0x01, 0, 8);
        Array.Fill(expected, (byte)0x02, 8, 8);
        Assert.Equal(expected, buffer.WrittenMemory.ToArray());
    }

    [Fact]
    public void NonGenericWriteColumn_NullEntry_ThrowsArgumentNull_WithIndex()
    {
        IColumnWriter sut = NewSut();
        var buffer = new ArrayBufferWriter<byte>();

        var ex = Assert.Throws<ArgumentNullException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            sut.WriteColumn(ref pw, new object?[] { NewState(0x01), null, NewState(0x03) });
        });
        Assert.Contains("index 1", ex.Message);
    }

    // --- Typed WriteColumn for completeness ----------------------------------

    [Fact]
    public void TypedWriteColumn_HappyPath_WritesAllRows()
    {
        var sut = NewSut();
        var values = new[] { NewState(0xAA), NewState(0xBB), NewState(0xCC) };

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        sut.WriteColumn(ref pw, values);

        Assert.Equal(24, buffer.WrittenCount);
    }
}

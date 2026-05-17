using System.Buffers;
using CH.Native.Data.AggregateState;
using CH.Native.Data.ColumnSkippers;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnSkippers;

/// <summary>
/// Direct unit coverage for <see cref="AggregateFunctionColumnSkipper"/>. The
/// round-trip skip parity is exercised elsewhere; this file targets the
/// row-loop boundary cases — zero rows, partial buffer (early-exit false),
/// and the FlagPlusFixed path where per-row size varies between 0x00-flag rows
/// and 0x01-prefixed rows.
/// </summary>
public class AggregateFunctionColumnSkipperTests
{
    private const string TypeName = "AggregateFunction(sum, Int32)";

    [Fact]
    public void TrySkipColumn_AllRowsAvailable_ReturnsTrue_ConsumesEverything()
    {
        // 3 rows × 8 bytes each — FixedSizeStateFormat(size=8).
        var sut = new AggregateFunctionColumnSkipper(TypeName, new FixedSizeStateFormat(size: 8));
        var bytes = new byte[24];
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        Assert.True(sut.TrySkipColumn(ref pr, rowCount: 3));
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void TrySkipColumn_PartialBuffer_ReturnsFalse()
    {
        // Asks for 3 rows worth (24 bytes) but only 16 available — the loop
        // succeeds on rows 0 and 1, then row 2's TrySkipOneState returns false.
        var sut = new AggregateFunctionColumnSkipper(TypeName, new FixedSizeStateFormat(size: 8));
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[16]));

        Assert.False(sut.TrySkipColumn(ref pr, rowCount: 3));
    }

    [Fact]
    public void TrySkipColumn_ZeroRows_ReturnsTrue_NoBytesConsumed()
    {
        // The for-loop body never executes; the method returns true immediately.
        // Buffer is non-empty but untouched.
        var sut = new AggregateFunctionColumnSkipper(TypeName, new FixedSizeStateFormat(size: 8));
        var bytes = new byte[8];
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        Assert.True(sut.TrySkipColumn(ref pr, rowCount: 0));
        Assert.Equal(8, pr.Remaining);
    }

    [Fact]
    public void TrySkipColumn_FlagPlusFixedFormat_VariableSizeRows_HandlesMixed()
    {
        // The skipper's loop must call TrySkipOneState for each row regardless of
        // per-row size variance. Row 0 is a 0x00-flag (1 byte); rows 1-2 are
        // 0x01-prefixed with 4 inner bytes each (5 bytes each).
        var sut = new AggregateFunctionColumnSkipper(
            "AggregateFunction(min, Int32)", new FlagPlusFixedStateFormat(innerSize: 4));

        var buffer = new byte[]
        {
            0x00,                          // row 0: empty group
            0x01, 0xAA, 0xBB, 0xCC, 0xDD,  // row 1: has value
            0x01, 0x11, 0x22, 0x33, 0x44,  // row 2: has value
        };
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer));

        Assert.True(sut.TrySkipColumn(ref pr, rowCount: 3));
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void TrySkipColumn_FlagPlusFixedFormat_MalformedFlagMidColumn_ReturnsFalse()
    {
        // Row 0 OK (flag=0x00). Row 1 has a malformed flag (0x42).
        // The skipper's per-row TrySkipOneState returns false, the column loop
        // surfaces that. Verifies the early-exit propagation works.
        var sut = new AggregateFunctionColumnSkipper(
            "AggregateFunction(min, Int32)", new FlagPlusFixedStateFormat(innerSize: 4));

        var buffer = new byte[]
        {
            0x00,                          // row 0: empty group — skip succeeds
            0x42, 0xAA, 0xBB, 0xCC, 0xDD,  // row 1: malformed flag — skip returns false
        };
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer));

        Assert.False(sut.TrySkipColumn(ref pr, rowCount: 2));
    }

    [Fact]
    public void TypeName_Getter_ReturnsConfiguredValue()
    {
        var sut = new AggregateFunctionColumnSkipper(TypeName, new FixedSizeStateFormat(size: 8));
        Assert.Equal(TypeName, sut.TypeName);
    }
}

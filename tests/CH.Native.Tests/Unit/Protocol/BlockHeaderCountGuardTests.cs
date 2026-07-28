using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol;

/// <summary>
/// Pins the decision table inside <see cref="ProtocolGuards.ValidateCountAgainstRemaining"/>
/// and <see cref="ProtocolGuards.ValidateBlockHeaderCounts"/>: which wire-declared
/// counts are terminal (<see cref="ClickHouseProtocolException"/>) versus a retryable
/// need-more-bytes signal (<c>ClickHouseCountGuardException</c>).
/// </summary>
/// <remarks>
/// The retryable/terminal split is load-bearing for liveness, not just correctness.
/// The compressed accumulate loop parses after every decompressed chunk, so
/// "declared count exceeds bytes available" is the NORMAL state of a legitimate
/// large block and must not condemn the connection. But two cases can never be
/// satisfied by more chunks arriving, and staying retryable in either one parks
/// the read pump until the caller's timeout fires:
/// <list type="bullet">
/// <item>a requirement of >= 2 GiB (the accumulate buffer is int-indexed), and</item>
/// <item>a column count above the structural ceiling (no amount of data makes a
/// 2-billion-column block legitimate).</item>
/// </list>
/// Those are exactly the branches these tests cover — a regression there turns a
/// corrupt or hostile header into a hang rather than an exception.
/// </remarks>
public class BlockHeaderCountGuardTests
{
    private static ProtocolReader ReaderOver(int byteCount)
        => new(new ReadOnlySequence<byte>(new byte[byteCount]));

    // ---- ValidateCountAgainstRemaining: terminal vs retryable ---------------

    [Fact]
    public void CountExceedsRemaining_NotRetryable_IsTerminal()
    {
        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
            ProtocolGuards.ValidateCountAgainstRemaining(
                count: 1_000, minBytesPerItem: 1, remaining: 10, fieldName: "test count"));

        Assert.Contains("test count", ex.Message);
        Assert.Contains("1000", ex.Message);
    }

    [Fact]
    public void CountExceedsRemaining_Retryable_SignalsDeficit()
    {
        // The accumulate loop uses DeficitBytes to skip re-parses until at least
        // that many new bytes have been decompressed, so the arithmetic matters:
        // count * minBytesPerItem - remaining.
        var ex = Assert.Throws<ClickHouseCountGuardException>(() =>
            ProtocolGuards.ValidateCountAgainstRemaining(
                count: 1_000, minBytesPerItem: 4, remaining: 100, fieldName: "test count", retryable: true));

        Assert.Equal(3_900, ex.DeficitBytes);
    }

    [Fact]
    public void CountRequiringAtLeast2GiB_Retryable_IsStillTerminal()
    {
        // THE anti-hang rule. The accumulate loop assembles chunks into one
        // int-indexed buffer, so a requirement above int.MaxValue can never be
        // satisfied no matter how many chunks follow. Returning the retryable
        // signal here would park the read pump waiting for bytes that cannot help.
        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
            ProtocolGuards.ValidateCountAgainstRemaining(
                count: 1_100_000_000, minBytesPerItem: 2, remaining: 64, fieldName: "test count", retryable: true));

        Assert.Contains("malformed or hostile", ex.Message);
    }

    [Fact]
    public void CountRequiringExactlyIntMaxValue_Retryable_StaysRetryable()
    {
        // Boundary partner to the test above: the cutoff is `required <= int.MaxValue`,
        // so a requirement of exactly int.MaxValue is still (just) satisfiable and
        // must keep the retryable signal.
        var ex = Assert.Throws<ClickHouseCountGuardException>(() =>
            ProtocolGuards.ValidateCountAgainstRemaining(
                count: int.MaxValue, minBytesPerItem: 1, remaining: 0, fieldName: "test count", retryable: true));

        Assert.Equal(int.MaxValue, ex.DeficitBytes);
    }

    [Theory]
    [InlineData(0, 1, 0)]      // zero count never trips, even with nothing available
    [InlineData(10, 4, 40)]    // requirement exactly met
    [InlineData(10, 4, 4096)]  // plenty available
    public void PlausibleCount_DoesNotThrow(int count, int minBytesPerItem, long remaining)
    {
        ProtocolGuards.ValidateCountAgainstRemaining(count, minBytesPerItem, remaining, "test count");
        ProtocolGuards.ValidateCountAgainstRemaining(count, minBytesPerItem, remaining, "test count", retryable: true);
    }

    // ---- ValidateBlockHeaderCounts: structural ceiling ----------------------

    [Fact]
    public void ColumnCountAboveStructuralCeiling_IsTerminalEvenWhenRetryable()
    {
        // Count-INDEPENDENT verdict: more data arriving cannot make this header
        // legitimate. The accumulate loop needs this precisely because it cannot
        // otherwise tell "the declared counts are bogus" from "the rest of the
        // block is still in flight" — without it, a hostile header spins in the
        // need-more-data retry until the caller's timeout.
        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
            ProtocolGuards.ValidateBlockHeaderCounts(
                columnCount: ProtocolGuards.MaxPlausibleColumnCount + 1,
                rowCount: 1,
                remaining: long.MaxValue,
                retryable: true));

        Assert.Contains("structural maximum", ex.Message);
    }

    [Fact]
    public void ColumnCountAtStructuralCeiling_PassesTheCeilingCheck()
    {
        // At the ceiling the structural check must pass; with the bytes to back it
        // the whole guard passes. (2 bytes/column minimum × 65536 = 131072.)
        ProtocolGuards.ValidateBlockHeaderCounts(
            columnCount: ProtocolGuards.MaxPlausibleColumnCount,
            rowCount: 0,
            remaining: long.MaxValue);
    }

    [Fact]
    public void RowCountCostsOneBytePerColumn()
    {
        // Row cost is per-row-per-column: 1000 rows × 4 columns needs >= 4000 bytes.
        var ex = Assert.Throws<ClickHouseProtocolException>(() =>
            ProtocolGuards.ValidateBlockHeaderCounts(columnCount: 4, rowCount: 1_000, remaining: 3_999));

        Assert.Contains("block rowCount", ex.Message);
    }

    // ---- the Block.ReadColumnsWithHeader call site --------------------------

    [Fact]
    public void ReadColumnsWithHeader_ImplausibleColumnCount_ThrowsBeforeAllocating()
    {
        // The pre-read-header entry point (counts supplied by the caller rather
        // than parsed here) shares the same gate. Without it, this call would
        // allocate three 100-million-element arrays from a 4-byte buffer.
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var reader = ReaderOver(4);
            Block.ReadColumnsWithHeader(
                ref reader, ColumnReaderRegistry.Default, "t", columnCount: 100_000_000, rowCount: 0);
        });
    }

    [Fact]
    public void ReadColumnsWithHeader_ImplausibleRowCount_ThrowsBeforeAllocating()
    {
        Assert.Throws<ClickHouseProtocolException>(() =>
        {
            var reader = ReaderOver(16);
            Block.ReadColumnsWithHeader(
                ref reader, ColumnReaderRegistry.Default, "t", columnCount: 2, rowCount: 500_000_000);
        });
    }
}

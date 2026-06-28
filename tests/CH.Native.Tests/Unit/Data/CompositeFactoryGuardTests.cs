using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Guard/error arms of the writer and skipper factories for composite types — the paths
/// the happy-path round-trip tests don't reach. Raw <c>AggregateFunction(...)</c> state
/// columns are not writable/skippable (opaque, server-internal) and must fail with the
/// finalizeAggregation/hex guidance; a <c>Nested(...)</c> declared without field names is
/// malformed and must be rejected at construction.
/// </summary>
public class CompositeFactoryGuardTests
{
    [Fact]
    public void Writer_AggregateFunction_ThrowsNotSupported_WithGuidance()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            ColumnWriterRegistry.Default.GetWriter("AggregateFunction(sum, Int32)"));
        Assert.Contains("not supported", ex.Message);
        Assert.Contains("finalizeAggregation", ex.Message);
    }

    [Fact]
    public void Skipper_AggregateFunction_ThrowsNotSupported_WithGuidance()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            ColumnSkipperRegistry.Default.GetSkipper("AggregateFunction(sum, Int32)"));
        Assert.Contains("not supported", ex.Message);
        Assert.Contains("finalizeAggregation", ex.Message);
    }

    [Fact]
    public void Writer_NestedWithoutFieldNames_ThrowsFormat()
    {
        // The writer needs field names to render the Nested(...) declaration, so an
        // unnamed Nested(...) is rejected at construction. (The skipper, by contrast,
        // only consumes bytes and does not require names — so it does not reject this.)
        var ex = Assert.Throws<FormatException>(() =>
            ColumnWriterRegistry.Default.GetWriter("Nested(String, Int32)"));
        Assert.Contains("Nested", ex.Message);
    }
}

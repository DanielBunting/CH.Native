using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Regression tests for Nested(...) reader/writer parity — release-prep item 2
/// (03-nested-writer.md). The ClickHouse <c>Nested(...)</c> type used to be readable
/// and skippable but not bulk-insertable (the writer registry threw
/// <see cref="NotSupportedException"/>), violating the project invariant that a
/// composite type resolves across the reader / writer / skipper registries. These
/// pin that a writer now resolves and reproduces the Nested type name.
/// </summary>
public class NestedColumnWriterTests
{
    private const string NestedType = "Nested(key String, value Int32)";

    [Fact]
    public void Reader_ResolvesNested()
    {
        var reader = ColumnReaderRegistry.Default.GetReader(NestedType);
        Assert.NotNull(reader);
    }

    [Fact]
    public void Writer_ResolvesNested_ParityWithReader()
    {
        // Previously threw NotSupportedException("... is not supported for writing.").
        var writer = ColumnWriterRegistry.Default.GetWriter(NestedType);
        Assert.NotNull(writer);
    }

    [Fact]
    public void Writer_ReproducesNestedTypeName_MatchingReader()
    {
        // A genuine Nested writer (not a coincidental fallthrough) renders the
        // Nested(...) declaration, and round-trips to the same type name the reader
        // produces — the strongest server-independent parity signal.
        var reader = ColumnReaderRegistry.Default.GetReader(NestedType);
        var writer = ColumnWriterRegistry.Default.GetWriter(NestedType);

        Assert.StartsWith("Nested(", writer.TypeName);
        Assert.Contains("key", writer.TypeName);
        Assert.Contains("value", writer.TypeName);
        Assert.Equal(reader.TypeName, writer.TypeName);
    }

    // ------------------------------------------------------------------
    // Validation surface. These error paths are the most likely user-facing
    // failures for new write code, and each carries a bespoke message — they
    // must actually fire (and name the offending row/field) rather than slip
    // through and desync the block stream. Column data on the wire is not
    // length-prefixed, so a writer that silently mis-shapes a row would
    // corrupt every following column (see ConnectionRecoveryTests). We assert
    // the throw happens up front, before any bytes commit.
    // ------------------------------------------------------------------

    // ProtocolWriter is a ref struct, so it can't be captured by an Assert.Throws
    // lambda; drive the write inside a try and hand back the caught exception.
    private static InvalidOperationException CaptureWriteColumnThrow(string nestedType, object?[] rows)
    {
        var writer = ColumnWriterRegistry.Default.GetWriter(nestedType);
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WritePrefix(ref pw);
        try
        {
            writer.WriteColumn(ref pw, rows);
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }

        throw new Xunit.Sdk.XunitException("Expected InvalidOperationException, but the write succeeded.");
    }

    private static InvalidOperationException CaptureWriteValueThrow(string nestedType, object? value)
    {
        var writer = ColumnWriterRegistry.Default.GetWriter(nestedType);
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        writer.WritePrefix(ref pw);
        try
        {
            writer.WriteValue(ref pw, value);
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }

        throw new Xunit.Sdk.XunitException("Expected InvalidOperationException, but the write succeeded.");
    }

    [Fact]
    public void WriteColumn_NullRow_Throws_NamingTheRow()
    {
        // Nested columns are non-nullable; a null row must fail clean, not NRE.
        var rows = new object?[]
        {
            new object[] { new[] { "a" }, new[] { 1 } },
            null, // row 1 is null
        };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("received null", ex.Message);
        Assert.Contains("at row 1", ex.Message);
    }

    [Fact]
    public void WriteColumn_TooFewFields_ThrowsArityMismatch()
    {
        // Declares Nested(key, value) (2 fields) but the row supplies 1.
        var rows = new object?[]
        {
            new object[] { new[] { "a", "b" } },
        };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("has 1 fields", ex.Message);
        Assert.Contains("declares 2", ex.Message);
        Assert.Contains("field count must match exactly", ex.Message);
    }

    [Fact]
    public void WriteColumn_TooManyFields_ThrowsArityMismatch()
    {
        var rows = new object?[]
        {
            new object[] { new[] { "a" }, new[] { 1 }, new[] { 9.9 } }, // 3 fields
        };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("has 3 fields", ex.Message);
        Assert.Contains("declares 2", ex.Message);
    }

    [Fact]
    public void WriteColumn_RaggedFields_Throws_NamingBothFieldsAndRow()
    {
        // key has 2 elements, value has 1 — they share one offsets block, so the
        // lengths must agree. This is the bug that silently truncated data before.
        var rows = new object?[]
        {
            new object[] { new[] { "a", "b" }, new[] { 1 } },
        };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("ragged", ex.Message);
        Assert.Contains("key", ex.Message);
        Assert.Contains("value", ex.Message);
        Assert.Contains("same length", ex.Message);
    }

    [Fact]
    public void WriteColumn_FieldNotAnArray_Throws()
    {
        // A scalar where the per-field array is expected — string is IEnumerable<char>
        // but not IList, so it must be rejected, not silently iterated.
        var rows = new object?[]
        {
            new object[] { "scalar-not-array", new[] { 1 } },
        };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("key", ex.Message);
        Assert.Contains("must be an array", ex.Message);
    }

    [Fact]
    public void WriteColumn_RowNotObjectArray_Throws()
    {
        // The non-generic entry point requires each row to be object[] of per-field
        // arrays; a bare value must be rejected with a type-named message.
        var rows = new object?[] { "not-a-row" };

        var ex = CaptureWriteColumnThrow(NestedType, rows);
        Assert.Contains("unsupported value type", ex.Message);
        Assert.Contains("String", ex.Message);
        Assert.Contains("object[]", ex.Message);
    }

    [Fact]
    public void WriteValue_Null_Throws()
    {
        var ex = CaptureWriteValueThrow(NestedType, null);
        Assert.Contains("received null", ex.Message);
    }

    [Fact]
    public void WriteValue_ArityMismatch_Throws()
    {
        var ex = CaptureWriteValueThrow(NestedType, new object[] { new[] { 1 } });
        Assert.Contains("field count must match exactly", ex.Message);
    }

    [Fact]
    public void WriteValue_Ragged_Throws()
    {
        var ex = CaptureWriteValueThrow(
            NestedType,
            new object[] { new[] { "a", "b" }, new[] { 1 } });
        Assert.Contains("ragged", ex.Message);
    }

    [Fact]
    public void Metadata_ClrType_FieldCount_FieldNames()
    {
        var w = new NestedColumnWriter(
            new IColumnWriter[] { new Int32ColumnWriter(), new StringColumnWriter() },
            new[] { "id", "name" });

        Assert.Equal(typeof(object[]), w.ClrType);
        Assert.Equal(2, w.FieldCount);
        Assert.Equal(new[] { "id", "name" }, w.FieldNames);
    }

    [Fact]
    public void Ctor_NoFieldWriters_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            new NestedColumnWriter(System.Array.Empty<IColumnWriter>(), System.Array.Empty<string>()));
        Assert.Contains("at least one field", ex.Message);
    }

    [Fact]
    public void Ctor_NameCountMismatch_Throws()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            new NestedColumnWriter(
                new IColumnWriter[] { new Int32ColumnWriter() },
                new[] { "a", "b" }));
        Assert.Contains("Field names count must match", ex.Message);
    }
}

using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Audit finding #18: Cross-checks for TupleColumnReader element row counts.
///
/// TupleColumnReader reads each element as a separate columnar block sized to
/// rowCount; the audit suggested adding defensive assertions that each element
/// reader actually advances by rowCount of payload. These tests document
/// observed behaviour: well-formed input round-trips, and an under-reading
/// inner reader leaves the protocol stream out of sync — but no exception is
/// raised by TupleColumnReader itself today.
/// </summary>
public class TupleColumnReaderTests
{
    private static readonly ColumnReaderFactory ReaderFactory = new(ColumnReaderRegistry.Default);

    [Fact]
    public void ReadTypedColumn_PositionalIntStringTuple_ReadsAllRows()
    {
        var reader = (TupleColumnReader)ReaderFactory.CreateReader("Tuple(Int64, String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // Element 0: 3x Int64
        writer.WriteInt64(1);
        writer.WriteInt64(2);
        writer.WriteInt64(3);
        // Element 1: 3x String
        writer.WriteString("a");
        writer.WriteString("b");
        writer.WriteString("c");

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 3);

        Assert.Equal(3, col.Count);
        var row0 = (Tuple<long, string>)col.GetValue(0)!;
        var row2 = (Tuple<long, string>)col.GetValue(2)!;
        Assert.Equal(1L, row0.Item1);
        Assert.Equal("a", row0.Item2);
        Assert.Equal(3L, row2.Item1);
        Assert.Equal("c", row2.Item2);
    }

    [Fact]
    public void ReadTypedColumn_NamedTuple_PreservesFieldOrder()
    {
        var reader = (TupleColumnReader)ReaderFactory.CreateReader("Tuple(id Int32, name String)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        writer.WriteInt32(7);
        writer.WriteInt32(8);
        writer.WriteString("x");
        writer.WriteString("y");

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        using var col = reader.ReadTypedColumn(ref pr, 2);

        var first = (Tuple<int, string>)col.GetValue(0)!;
        Assert.Equal(7, first.Item1);
        Assert.Equal("x", first.Item2);
        Assert.True(reader.HasFieldNames);
        Assert.Equal(0, reader.GetFieldIndex("id"));
        Assert.Equal(1, reader.GetFieldIndex("name"));
    }

    // Audit finding #18: TupleColumnReader should cross-check that each element
    // reader's column reports the requested rowCount, so a malformed inner read
    // surfaces as a typed "tuple element under-read" diagnostic rather than a
    // generic ProtocolReader "not enough data" error from whatever happens to
    // read next. This test FAILS until that defensive check is added: it expects
    // either a typed InvalidDataException OR an exception whose message names
    // the offending tuple element, and rejects the bare InvalidOperationException
    // we get today.
    [Fact]
    public void ReadTypedColumn_TruncatedSecondElementPayload_ReportsTupleElementMismatch()
    {
        var reader = (TupleColumnReader)ReaderFactory.CreateReader("Tuple(Int64, Int64)");

        using var buffer = new PooledBufferWriter();
        var writer = new ProtocolWriter(buffer);
        // Element 0: 3 Int64s as expected
        writer.WriteInt64(1);
        writer.WriteInt64(2);
        writer.WriteInt64(3);
        // Element 1: only 2 Int64s instead of 3 — under-read by 8 bytes
        writer.WriteInt64(10);
        writer.WriteInt64(20);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        // ProtocolReader is a ref struct — capture the exception via try/catch.
        Exception? thrown = null;
        try { _ = reader.ReadTypedColumn(ref pr, 3); }
        catch (Exception ex) { thrown = ex; }

        Assert.NotNull(thrown);

        // Desired: a typed protocol error or one that names the tuple element.
        // Until the cross-check is added, the bare InvalidOperationException
        // from ProtocolReader does NOT satisfy this and the test fails.
        var isTyped = thrown is InvalidDataException;
        var mentionsTuple = thrown.Message.Contains("Tuple", StringComparison.OrdinalIgnoreCase)
                            || thrown.Message.Contains("element", StringComparison.OrdinalIgnoreCase);
        Assert.True(isTyped || mentionsTuple,
            $"Expected typed protocol error or tuple-element diagnostic; got {thrown.GetType().Name}: {thrown.Message}");
    }

    [Fact]
    public void Arity_AndTypeName_MatchConstructorInputs()
    {
        var reader = (TupleColumnReader)ReaderFactory.CreateReader("Tuple(Int32, String, UInt8)");
        Assert.Equal(3, reader.Arity);
        Assert.Equal("Tuple(Int32, String, UInt8)", reader.TypeName);
    }
}

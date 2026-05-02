using System.Buffers;
using CH.Native.Data.ColumnReaders;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// JsonStringColumnReader expects a column-level prefix carrying the JSON
/// serialization version. Version 1 (string format) is supported; versions 0
/// and 3 (object formats) and any unknown version throw NotSupportedException
/// with a clear message. Pin both the happy path and the rejection branches —
/// the rejection messages are the only signal a user gets when they forget to
/// flip the server-side flag.
/// </summary>
public class JsonStringColumnReaderTests
{
    [Fact]
    public void TypeName_IsJSON() => Assert.Equal("JSON", new JsonStringColumnReader().TypeName);

    [Fact]
    public void ClrType_IsString() => Assert.Equal(typeof(string), new JsonStringColumnReader().ClrType);

    private static byte[] BuildPrefixedColumn(ulong version, string[] payloads)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteUInt64(version);
        foreach (var s in payloads) writer.WriteString(s);
        return buffer.WrittenSpan.ToArray();
    }

    [Fact]
    public void Version1_String_DecodesPayload()
    {
        var bytes = BuildPrefixedColumn(version: 1, new[] { "{\"a\":1}", "[]", "\"hello\"" });
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var sut = new JsonStringColumnReader();
        sut.ReadPrefix(ref reader);
        using var column = sut.ReadTypedColumn(ref reader, 3);

        Assert.Equal("{\"a\":1}", column[0]);
        Assert.Equal("[]", column[1]);
        Assert.Equal("\"hello\"", column[2]);
    }

    private static NotSupportedException CaptureReadFailure(ulong version)
    {
        // ProtocolReader is a ref struct so we can't capture it in a lambda
        // (which Assert.Throws requires). Pattern: try/catch and assert NotNull.
        var bytes = BuildPrefixedColumn(version, Array.Empty<string>());
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var sut = new JsonStringColumnReader();
        sut.ReadPrefix(ref reader);

        NotSupportedException? caught = null;
        try { _ = sut.ReadTypedColumn(ref reader, 0); }
        catch (NotSupportedException ex) { caught = ex; }
        Assert.NotNull(caught);
        return caught!;
    }

    [Fact]
    public void Version0_Object_ThrowsWithSettingsHint()
    {
        Assert.Contains("output_format_native_write_json_as_string", CaptureReadFailure(0).Message);
    }

    [Fact]
    public void Version3_Object_ThrowsWithSettingsHint()
    {
        Assert.Contains("output_format_native_write_json_as_string", CaptureReadFailure(3).Message);
    }

    [Fact]
    public void UnknownVersion_ThrowsWithVersionInMessage()
    {
        Assert.Contains("42", CaptureReadFailure(42).Message);
    }

    [Fact]
    public void Version1_LargeBlock_TriggersInternPath()
    {
        // The intern path activates for rowCount >= 100. Repeated payloads
        // should still decode equal — the intern is internal, observable only
        // via correctness of the result.
        var payloads = new string[200];
        for (int i = 0; i < payloads.Length; i++)
            payloads[i] = (i % 3) switch
            {
                0 => "{\"a\":1}",
                1 => "{\"a\":2}",
                _ => "{\"a\":3}",
            };

        var bytes = BuildPrefixedColumn(version: 1, payloads);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var sut = new JsonStringColumnReader();
        sut.ReadPrefix(ref reader);
        using var column = sut.ReadTypedColumn(ref reader, payloads.Length);

        for (int i = 0; i < payloads.Length; i++) Assert.Equal(payloads[i], column[i]);
    }

    [Fact]
    public void ReadValue_DecodesSingleString()
    {
        // ReadValue is independent of the prefix — it reads a single VarInt-prefixed string.
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        writer.WriteString("{\"k\":42}");
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));

        var result = new JsonStringColumnReader().ReadValue(ref reader);

        Assert.Equal("{\"k\":42}", result);
    }
}

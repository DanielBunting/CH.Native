using CH.Native.Protocol;

namespace CH.Native.Data.ColumnReaders;

/// <summary>
/// Column reader for the Nothing type — the inner type of a bare <c>SELECT NULL</c>
/// (Nullable(Nothing)) or <c>SELECT []</c> (Array(Nothing)).
/// Nothing carries no information: the wire format is one dummy byte per row and every
/// value is null. Supporting it matters because ORMs and health checks emit bare
/// <c>SELECT NULL</c> probes.
/// </summary>
internal sealed class NothingColumnReader : IColumnReader<object?>
{
    /// <inheritdoc />
    public string TypeName => "Nothing";

    /// <inheritdoc />
    public Type ClrType => typeof(object);

    /// <inheritdoc />
    public object? ReadValue(ref ProtocolReader reader)
    {
        reader.ReadByte();
        return null;
    }

    /// <inheritdoc />
    public TypedColumn<object?> ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        for (int i = 0; i < rowCount; i++)
        {
            reader.ReadByte();
        }

        // A freshly allocated object?[] is already all-null; row counts here are tiny
        // (typically 1), so pooling would only add a mandatory clear-on-return.
        return new TypedColumn<object?>(rowCount == 0 ? Array.Empty<object?>() : new object?[rowCount]);
    }

    ITypedColumn IColumnReader.ReadTypedColumn(ref ProtocolReader reader, int rowCount)
    {
        return ReadTypedColumn(ref reader, rowCount);
    }
}

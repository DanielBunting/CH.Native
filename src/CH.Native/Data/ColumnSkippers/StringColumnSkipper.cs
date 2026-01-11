using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Column skipper for String values.
/// Reads varint length prefix for each string and skips the bytes.
/// </summary>
public sealed class StringColumnSkipper : IColumnSkipper
{
    public string TypeName => "String";

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        for (int i = 0; i < rowCount; i++)
        {
            if (!reader.TrySkipString())
                return false;
        }
        return true;
    }
}

/// <summary>
/// Column skipper for FixedString(N) values.
/// Each row is exactly N bytes.
/// </summary>
public sealed class FixedStringColumnSkipper : IColumnSkipper
{
    private readonly int _length;

    public FixedStringColumnSkipper(int length)
    {
        _length = length;
    }

    public string TypeName => $"FixedString({_length})";

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        return reader.TrySkipBytes((long)rowCount * _length);
    }
}

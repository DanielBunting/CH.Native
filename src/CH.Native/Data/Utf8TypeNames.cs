namespace CH.Native.Data;

/// <summary>
/// UTF-8 encoded type names for zero-allocation type comparison during scan pass.
/// Using u8 string literals allows direct byte comparison without string allocation.
/// </summary>
internal static class Utf8TypeNames
{
    // 1-byte types
    public static ReadOnlySpan<byte> Int8 => "Int8"u8;
    public static ReadOnlySpan<byte> UInt8 => "UInt8"u8;
    public static ReadOnlySpan<byte> Bool => "Bool"u8;

    // 2-byte types
    public static ReadOnlySpan<byte> Int16 => "Int16"u8;
    public static ReadOnlySpan<byte> UInt16 => "UInt16"u8;
    public static ReadOnlySpan<byte> Date => "Date"u8;

    // 4-byte types
    public static ReadOnlySpan<byte> Int32 => "Int32"u8;
    public static ReadOnlySpan<byte> UInt32 => "UInt32"u8;
    public static ReadOnlySpan<byte> Float32 => "Float32"u8;
    public static ReadOnlySpan<byte> DateTime => "DateTime"u8;
    public static ReadOnlySpan<byte> Date32 => "Date32"u8;
    public static ReadOnlySpan<byte> IPv4 => "IPv4"u8;

    // 8-byte types
    public static ReadOnlySpan<byte> Int64 => "Int64"u8;
    public static ReadOnlySpan<byte> UInt64 => "UInt64"u8;
    public static ReadOnlySpan<byte> Float64 => "Float64"u8;

    // 16-byte types
    public static ReadOnlySpan<byte> Int128 => "Int128"u8;
    public static ReadOnlySpan<byte> UInt128 => "UInt128"u8;
    public static ReadOnlySpan<byte> UUID => "UUID"u8;
    public static ReadOnlySpan<byte> IPv6 => "IPv6"u8;

    // 32-byte types
    public static ReadOnlySpan<byte> Int256 => "Int256"u8;
    public static ReadOnlySpan<byte> UInt256 => "UInt256"u8;

    // Variable-length types
    public static ReadOnlySpan<byte> String => "String"u8;

    // Enum types
    public static ReadOnlySpan<byte> Enum8 => "Enum8"u8;
    public static ReadOnlySpan<byte> Enum16 => "Enum16"u8;
}

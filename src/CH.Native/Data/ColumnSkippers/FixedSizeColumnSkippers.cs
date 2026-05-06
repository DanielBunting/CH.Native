using CH.Native.Protocol;

namespace CH.Native.Data.ColumnSkippers;

/// <summary>
/// Base class for fixed-size column skippers. Simply skips rowCount * byteSize bytes.
/// </summary>
public abstract class FixedSizeColumnSkipper : IColumnSkipper
{
    private readonly int _byteSize;

    protected FixedSizeColumnSkipper(int byteSize)
    {
        _byteSize = byteSize;
    }

    /// <inheritdoc />

    public abstract string TypeName { get; }

    /// <inheritdoc />

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        return reader.TrySkipBytes((long)rowCount * _byteSize);
    }
}

// 1-byte types
internal sealed class Int8ColumnSkipper : FixedSizeColumnSkipper
{
    public Int8ColumnSkipper() : base(1) { }
    /// <inheritdoc />
    public override string TypeName => "Int8";
}

internal sealed class UInt8ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt8ColumnSkipper() : base(1) { }
    /// <inheritdoc />
    public override string TypeName => "UInt8";
}

internal sealed class BoolColumnSkipper : FixedSizeColumnSkipper
{
    public BoolColumnSkipper() : base(1) { }
    /// <inheritdoc />
    public override string TypeName => "Bool";
}

// 2-byte types
internal sealed class Int16ColumnSkipper : FixedSizeColumnSkipper
{
    public Int16ColumnSkipper() : base(2) { }
    /// <inheritdoc />
    public override string TypeName => "Int16";
}

internal sealed class UInt16ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt16ColumnSkipper() : base(2) { }
    /// <inheritdoc />
    public override string TypeName => "UInt16";
}

internal sealed class DateColumnSkipper : FixedSizeColumnSkipper
{
    public DateColumnSkipper() : base(2) { }
    /// <inheritdoc />
    public override string TypeName => "Date";
}

internal sealed class BFloat16ColumnSkipper : FixedSizeColumnSkipper
{
    public BFloat16ColumnSkipper() : base(2) { }
    /// <inheritdoc />
    public override string TypeName => "BFloat16";
}

// 4-byte types
internal sealed class Int32ColumnSkipper : FixedSizeColumnSkipper
{
    public Int32ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "Int32";
}

internal sealed class UInt32ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt32ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "UInt32";
}

internal sealed class Float32ColumnSkipper : FixedSizeColumnSkipper
{
    public Float32ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "Float32";
}

internal sealed class DateTimeColumnSkipper : FixedSizeColumnSkipper
{
    public DateTimeColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "DateTime";
}

internal sealed class TimeColumnSkipper : FixedSizeColumnSkipper
{
    public TimeColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "Time";
}

internal sealed class Date32ColumnSkipper : FixedSizeColumnSkipper
{
    public Date32ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "Date32";
}

internal sealed class IPv4ColumnSkipper : FixedSizeColumnSkipper
{
    public IPv4ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "IPv4";
}

internal sealed class Decimal32ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal32ColumnSkipper() : base(4) { }
    /// <inheritdoc />
    public override string TypeName => "Decimal32";
}

// 8-byte types
internal sealed class Int64ColumnSkipper : FixedSizeColumnSkipper
{
    public Int64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "Int64";
}

internal sealed class UInt64ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "UInt64";
}

internal sealed class Float64ColumnSkipper : FixedSizeColumnSkipper
{
    public Float64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "Float64";
}

internal sealed class DateTime64ColumnSkipper : FixedSizeColumnSkipper
{
    public DateTime64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "DateTime64";
}

internal sealed class Time64ColumnSkipper : FixedSizeColumnSkipper
{
    public Time64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "Time64";
}

internal sealed class Decimal64ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal64ColumnSkipper() : base(8) { }
    /// <inheritdoc />
    public override string TypeName => "Decimal64";
}

// 16-byte types
internal sealed class Int128ColumnSkipper : FixedSizeColumnSkipper
{
    public Int128ColumnSkipper() : base(16) { }
    /// <inheritdoc />
    public override string TypeName => "Int128";
}

internal sealed class UInt128ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt128ColumnSkipper() : base(16) { }
    /// <inheritdoc />
    public override string TypeName => "UInt128";
}

internal sealed class UuidColumnSkipper : FixedSizeColumnSkipper
{
    public UuidColumnSkipper() : base(16) { }
    /// <inheritdoc />
    public override string TypeName => "UUID";
}

internal sealed class IPv6ColumnSkipper : FixedSizeColumnSkipper
{
    public IPv6ColumnSkipper() : base(16) { }
    /// <inheritdoc />
    public override string TypeName => "IPv6";
}

internal sealed class Decimal128ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal128ColumnSkipper() : base(16) { }
    /// <inheritdoc />
    public override string TypeName => "Decimal128";
}

// 32-byte types
internal sealed class Int256ColumnSkipper : FixedSizeColumnSkipper
{
    public Int256ColumnSkipper() : base(32) { }
    /// <inheritdoc />
    public override string TypeName => "Int256";
}

internal sealed class UInt256ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt256ColumnSkipper() : base(32) { }
    /// <inheritdoc />
    public override string TypeName => "UInt256";
}

internal sealed class Decimal256ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal256ColumnSkipper() : base(32) { }
    /// <inheritdoc />
    public override string TypeName => "Decimal256";
}

// Enum types (same as underlying integer types)
internal sealed class Enum8ColumnSkipper : FixedSizeColumnSkipper
{
    public Enum8ColumnSkipper() : base(1) { }
    /// <inheritdoc />
    public override string TypeName => "Enum8";
}

internal sealed class Enum16ColumnSkipper : FixedSizeColumnSkipper
{
    public Enum16ColumnSkipper() : base(2) { }
    /// <inheritdoc />
    public override string TypeName => "Enum16";
}

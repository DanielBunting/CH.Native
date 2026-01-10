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

    public abstract string TypeName { get; }

    public bool TrySkipColumn(ref ProtocolReader reader, int rowCount)
    {
        return reader.TrySkipBytes((long)rowCount * _byteSize);
    }
}

// 1-byte types
public sealed class Int8ColumnSkipper : FixedSizeColumnSkipper
{
    public Int8ColumnSkipper() : base(1) { }
    public override string TypeName => "Int8";
}

public sealed class UInt8ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt8ColumnSkipper() : base(1) { }
    public override string TypeName => "UInt8";
}

public sealed class BoolColumnSkipper : FixedSizeColumnSkipper
{
    public BoolColumnSkipper() : base(1) { }
    public override string TypeName => "Bool";
}

// 2-byte types
public sealed class Int16ColumnSkipper : FixedSizeColumnSkipper
{
    public Int16ColumnSkipper() : base(2) { }
    public override string TypeName => "Int16";
}

public sealed class UInt16ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt16ColumnSkipper() : base(2) { }
    public override string TypeName => "UInt16";
}

public sealed class DateColumnSkipper : FixedSizeColumnSkipper
{
    public DateColumnSkipper() : base(2) { }
    public override string TypeName => "Date";
}

// 4-byte types
public sealed class Int32ColumnSkipper : FixedSizeColumnSkipper
{
    public Int32ColumnSkipper() : base(4) { }
    public override string TypeName => "Int32";
}

public sealed class UInt32ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt32ColumnSkipper() : base(4) { }
    public override string TypeName => "UInt32";
}

public sealed class Float32ColumnSkipper : FixedSizeColumnSkipper
{
    public Float32ColumnSkipper() : base(4) { }
    public override string TypeName => "Float32";
}

public sealed class DateTimeColumnSkipper : FixedSizeColumnSkipper
{
    public DateTimeColumnSkipper() : base(4) { }
    public override string TypeName => "DateTime";
}

public sealed class Date32ColumnSkipper : FixedSizeColumnSkipper
{
    public Date32ColumnSkipper() : base(4) { }
    public override string TypeName => "Date32";
}

public sealed class IPv4ColumnSkipper : FixedSizeColumnSkipper
{
    public IPv4ColumnSkipper() : base(4) { }
    public override string TypeName => "IPv4";
}

public sealed class Decimal32ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal32ColumnSkipper() : base(4) { }
    public override string TypeName => "Decimal32";
}

// 8-byte types
public sealed class Int64ColumnSkipper : FixedSizeColumnSkipper
{
    public Int64ColumnSkipper() : base(8) { }
    public override string TypeName => "Int64";
}

public sealed class UInt64ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt64ColumnSkipper() : base(8) { }
    public override string TypeName => "UInt64";
}

public sealed class Float64ColumnSkipper : FixedSizeColumnSkipper
{
    public Float64ColumnSkipper() : base(8) { }
    public override string TypeName => "Float64";
}

public sealed class DateTime64ColumnSkipper : FixedSizeColumnSkipper
{
    public DateTime64ColumnSkipper() : base(8) { }
    public override string TypeName => "DateTime64";
}

public sealed class Decimal64ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal64ColumnSkipper() : base(8) { }
    public override string TypeName => "Decimal64";
}

// 16-byte types
public sealed class Int128ColumnSkipper : FixedSizeColumnSkipper
{
    public Int128ColumnSkipper() : base(16) { }
    public override string TypeName => "Int128";
}

public sealed class UInt128ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt128ColumnSkipper() : base(16) { }
    public override string TypeName => "UInt128";
}

public sealed class UuidColumnSkipper : FixedSizeColumnSkipper
{
    public UuidColumnSkipper() : base(16) { }
    public override string TypeName => "UUID";
}

public sealed class IPv6ColumnSkipper : FixedSizeColumnSkipper
{
    public IPv6ColumnSkipper() : base(16) { }
    public override string TypeName => "IPv6";
}

public sealed class Decimal128ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal128ColumnSkipper() : base(16) { }
    public override string TypeName => "Decimal128";
}

// 32-byte types
public sealed class Int256ColumnSkipper : FixedSizeColumnSkipper
{
    public Int256ColumnSkipper() : base(32) { }
    public override string TypeName => "Int256";
}

public sealed class UInt256ColumnSkipper : FixedSizeColumnSkipper
{
    public UInt256ColumnSkipper() : base(32) { }
    public override string TypeName => "UInt256";
}

public sealed class Decimal256ColumnSkipper : FixedSizeColumnSkipper
{
    public Decimal256ColumnSkipper() : base(32) { }
    public override string TypeName => "Decimal256";
}

// Enum types (same as underlying integer types)
public sealed class Enum8ColumnSkipper : FixedSizeColumnSkipper
{
    public Enum8ColumnSkipper() : base(1) { }
    public override string TypeName => "Enum8";
}

public sealed class Enum16ColumnSkipper : FixedSizeColumnSkipper
{
    public Enum16ColumnSkipper() : base(2) { }
    public override string TypeName => "Enum16";
}

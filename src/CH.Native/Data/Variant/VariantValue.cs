namespace CH.Native.Data.Variant;

/// <summary>
/// Boxing-free typed Variant value for the common 2-arm shape.
/// Use with <see cref="ColumnReaders.VariantColumnReader{T0, T1}"/>.
/// </summary>
public readonly struct VariantValue<T0, T1> : IEquatable<VariantValue<T0, T1>>
{
    public const byte NullDiscriminator = 255;

    private readonly T0 _v0;
    private readonly T1 _v1;

    public byte Discriminator { get; }

    public VariantValue(byte discriminator, T0 v0 = default!, T1 v1 = default!)
    {
        Discriminator = discriminator;
        _v0 = v0;
        _v1 = v1;
    }

    public static VariantValue<T0, T1> Null => new(NullDiscriminator);
    public static VariantValue<T0, T1> FromArm0(T0 value) => new(0, v0: value);
    public static VariantValue<T0, T1> FromArm1(T1 value) => new(1, v1: value);

    public bool IsNull => Discriminator == NullDiscriminator;

    public T0 Arm0 => Discriminator == 0
        ? _v0
        : throw new InvalidOperationException($"Variant value is arm {Discriminator}, not 0.");
    public T1 Arm1 => Discriminator == 1
        ? _v1
        : throw new InvalidOperationException($"Variant value is arm {Discriminator}, not 1.");

    public bool TryGetArm0(out T0 value)
    {
        if (Discriminator == 0) { value = _v0; return true; }
        value = default!;
        return false;
    }

    public bool TryGetArm1(out T1 value)
    {
        if (Discriminator == 1) { value = _v1; return true; }
        value = default!;
        return false;
    }

    /// <summary>
    /// Converts to the boxed <see cref="ClickHouseVariant"/> form for interop with APIs that
    /// require the object-valued representation.
    /// </summary>
    public ClickHouseVariant ToBoxed() => Discriminator switch
    {
        NullDiscriminator => ClickHouseVariant.Null,
        0 => new ClickHouseVariant(0, _v0),
        1 => new ClickHouseVariant(1, _v1),
        _ => throw new InvalidOperationException($"Unexpected discriminator {Discriminator}."),
    };

    public bool Equals(VariantValue<T0, T1> other)
    {
        if (Discriminator != other.Discriminator) return false;
        return Discriminator switch
        {
            0 => EqualityComparer<T0>.Default.Equals(_v0, other._v0),
            1 => EqualityComparer<T1>.Default.Equals(_v1, other._v1),
            _ => true, // NULL == NULL
        };
    }

    public override bool Equals(object? obj) => obj is VariantValue<T0, T1> v && Equals(v);
    public override int GetHashCode() => Discriminator switch
    {
        0 => HashCode.Combine((byte)0, _v0),
        1 => HashCode.Combine((byte)1, _v1),
        _ => 0,
    };
}

namespace CH.Native.Data.Variant;

/// <summary>
/// CLR representation of a single ClickHouse Variant(T1, T2, …) value.
/// </summary>
/// <remarks>
/// The <see cref="Discriminator"/> indexes into the declared arm list. A value of
/// <see cref="NullDiscriminator"/> (255) represents NULL and pairs with a null
/// <see cref="Value"/>.
///
/// Boxing of <see cref="Value"/> is unavoidable for the general multi-arm shape. Hot paths
/// that need zero-alloc reads should call <see cref="TryGetAs{T}"/> and keep the unboxed
/// value in local state.
/// </remarks>
public readonly struct ClickHouseVariant : IEquatable<ClickHouseVariant>
{
    /// <summary>
    /// Discriminator value used on the wire to mark a NULL row.
    /// </summary>
    public const byte NullDiscriminator = 255;

    /// <summary>
    /// Pre-constructed NULL value.
    /// </summary>
    public static ClickHouseVariant Null { get; } = new(NullDiscriminator, null);

    /// <summary>
    /// Zero-based index into the declared Variant arm list, or 255 for NULL.
    /// </summary>
    public byte Discriminator { get; }

    /// <summary>
    /// The inner value, boxed if a value type. Null iff <see cref="Discriminator"/> == 255.
    /// </summary>
    public object? Value { get; }

    public ClickHouseVariant(byte discriminator, object? value)
    {
        Discriminator = discriminator;
        Value = value;
    }

    /// <summary>
    /// Convenience factory.
    /// </summary>
    public static ClickHouseVariant Of(byte discriminator, object? value) => new(discriminator, value);

    /// <summary>
    /// Attempts to cast <see cref="Value"/> to the requested CLR type.
    /// Returns false for NULL or a type mismatch.
    /// </summary>
    public bool TryGetAs<T>(out T? value)
    {
        if (Discriminator == NullDiscriminator || Value is null)
        {
            value = default;
            return false;
        }

        if (Value is T typed)
        {
            value = typed;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// True when this value represents NULL.
    /// </summary>
    public bool IsNull => Discriminator == NullDiscriminator;

    /// <inheritdoc />
    public bool Equals(ClickHouseVariant other)
    {
        if (Discriminator != other.Discriminator) return false;
        if (Value is null) return other.Value is null;
        return Value.Equals(other.Value);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is ClickHouseVariant other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Discriminator, Value);

    public static bool operator ==(ClickHouseVariant left, ClickHouseVariant right) => left.Equals(right);
    public static bool operator !=(ClickHouseVariant left, ClickHouseVariant right) => !left.Equals(right);

    /// <inheritdoc />
    public override string ToString() => IsNull ? "NULL" : $"Variant[{Discriminator}]={Value}";
}

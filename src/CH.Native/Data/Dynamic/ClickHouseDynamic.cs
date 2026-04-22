using CH.Native.Data.Variant;

namespace CH.Native.Data.Dynamic;

/// <summary>
/// CLR representation of a single <c>Dynamic</c> value.
/// </summary>
/// <remarks>
/// Dynamic is a self-describing variant: the arm type list is supplied per-block by the server
/// rather than declared at schema time. <see cref="DeclaredTypeName"/> is the ClickHouse type
/// name of the arm that produced this row (e.g. <c>"Int64"</c>, <c>"Array(Int32)"</c>), or
/// <c>null</c> when the row is NULL.
/// </remarks>
public readonly struct ClickHouseDynamic : IEquatable<ClickHouseDynamic>
{
    /// <summary>
    /// Discriminator value used on the wire to mark a NULL row.
    /// </summary>
    public const byte NullDiscriminator = ClickHouseVariant.NullDiscriminator;

    /// <summary>
    /// Pre-constructed NULL value.
    /// </summary>
    public static ClickHouseDynamic Null { get; } = new(NullDiscriminator, null, null);

    /// <summary>
    /// Block-local arm index, or 255 for NULL. Note that the numeric value is only meaningful
    /// within the block that produced it — use <see cref="DeclaredTypeName"/> for stable identity.
    /// </summary>
    public byte Discriminator { get; }

    /// <summary>
    /// The inner value, boxed if a value type. Null iff <see cref="Discriminator"/> == 255.
    /// </summary>
    public object? Value { get; }

    /// <summary>
    /// ClickHouse type name of the arm that produced this row, or <c>null</c> for NULL rows.
    /// </summary>
    public string? DeclaredTypeName { get; }

    public ClickHouseDynamic(byte discriminator, object? value, string? declaredTypeName)
    {
        Discriminator = discriminator;
        Value = value;
        DeclaredTypeName = declaredTypeName;
    }

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
    public bool Equals(ClickHouseDynamic other)
    {
        if (Discriminator != other.Discriminator) return false;
        if (!string.Equals(DeclaredTypeName, other.DeclaredTypeName, StringComparison.Ordinal)) return false;
        if (Value is null) return other.Value is null;
        return Value.Equals(other.Value);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is ClickHouseDynamic other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Discriminator, DeclaredTypeName, Value);

    public static bool operator ==(ClickHouseDynamic left, ClickHouseDynamic right) => left.Equals(right);
    public static bool operator !=(ClickHouseDynamic left, ClickHouseDynamic right) => !left.Equals(right);

    /// <inheritdoc />
    public override string ToString() =>
        IsNull ? "NULL" : $"Dynamic[{DeclaredTypeName}]={Value}";
}

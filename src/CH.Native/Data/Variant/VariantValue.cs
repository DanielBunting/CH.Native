namespace CH.Native.Data.Variant;

/// <summary>
/// Boxing-free typed Variant value for the common 2-arm shape.
/// Use with <see cref="ColumnReaders.VariantColumnReader{T0, T1}"/>.
/// </summary>
/// <typeparam name="T0">The CLR type of the first Variant arm (discriminator <c>0</c>).</typeparam>
/// <typeparam name="T1">The CLR type of the second Variant arm (discriminator <c>1</c>).</typeparam>
public readonly struct VariantValue<T0, T1> : IEquatable<VariantValue<T0, T1>>
{
    /// <summary>
    /// The discriminator value that marks a Variant as holding NULL rather than either arm.
    /// </summary>
    public const byte NullDiscriminator = 255;

    private readonly T0 _v0;
    private readonly T1 _v1;

    /// <summary>
    /// The arm discriminator: <c>0</c> for <typeparamref name="T0"/>, <c>1</c> for
    /// <typeparamref name="T1"/>, or <see cref="NullDiscriminator"/> for NULL.
    /// </summary>
    public byte Discriminator { get; }

    /// <summary>
    /// Initializes a new <see cref="VariantValue{T0, T1}"/> with the given discriminator and arm values.
    /// </summary>
    /// <param name="discriminator">
    /// The arm discriminator: <c>0</c> for <typeparamref name="T0"/>, <c>1</c> for
    /// <typeparamref name="T1"/>, or <see cref="NullDiscriminator"/> for NULL.
    /// </param>
    /// <param name="v0">The <typeparamref name="T0"/> value; only meaningful when <paramref name="discriminator"/> is <c>0</c>.</param>
    /// <param name="v1">The <typeparamref name="T1"/> value; only meaningful when <paramref name="discriminator"/> is <c>1</c>.</param>
    public VariantValue(byte discriminator, T0 v0 = default!, T1 v1 = default!)
    {
        Discriminator = discriminator;
        _v0 = v0;
        _v1 = v1;
    }

    /// <summary>
    /// Gets a <see cref="VariantValue{T0, T1}"/> representing the NULL value.
    /// </summary>
    public static VariantValue<T0, T1> Null => new(NullDiscriminator);

    /// <summary>
    /// Creates a <see cref="VariantValue{T0, T1}"/> holding the first arm (<typeparamref name="T0"/>).
    /// </summary>
    /// <param name="value">The <typeparamref name="T0"/> value to wrap.</param>
    /// <returns>A Variant with discriminator <c>0</c>.</returns>
    public static VariantValue<T0, T1> FromArm0(T0 value) => new(0, v0: value);

    /// <summary>
    /// Creates a <see cref="VariantValue{T0, T1}"/> holding the second arm (<typeparamref name="T1"/>).
    /// </summary>
    /// <param name="value">The <typeparamref name="T1"/> value to wrap.</param>
    /// <returns>A Variant with discriminator <c>1</c>.</returns>
    public static VariantValue<T0, T1> FromArm1(T1 value) => new(1, v1: value);

    /// <summary>
    /// Gets a value indicating whether this Variant holds NULL (its <see cref="Discriminator"/> equals <see cref="NullDiscriminator"/>).
    /// </summary>
    public bool IsNull => Discriminator == NullDiscriminator;

    /// <summary>
    /// Gets the first arm value (<typeparamref name="T0"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the Variant does not hold arm <c>0</c> (i.e. it holds arm <c>1</c> or NULL).</exception>
    public T0 Arm0 => Discriminator == 0
        ? _v0
        : throw new InvalidOperationException($"Variant value is arm {Discriminator}, not 0.");

    /// <summary>
    /// Gets the second arm value (<typeparamref name="T1"/>).
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the Variant does not hold arm <c>1</c> (i.e. it holds arm <c>0</c> or NULL).</exception>
    public T1 Arm1 => Discriminator == 1
        ? _v1
        : throw new InvalidOperationException($"Variant value is arm {Discriminator}, not 1.");

    /// <summary>
    /// Attempts to get the first arm value (<typeparamref name="T0"/>) without throwing.
    /// </summary>
    /// <param name="value">When this method returns <see langword="true"/>, contains the <typeparamref name="T0"/> value; otherwise the default of <typeparamref name="T0"/>.</param>
    /// <returns><see langword="true"/> if the Variant holds arm <c>0</c>; otherwise <see langword="false"/>.</returns>
    public bool TryGetArm0(out T0 value)
    {
        if (Discriminator == 0) { value = _v0; return true; }
        value = default!;
        return false;
    }

    /// <summary>
    /// Attempts to get the second arm value (<typeparamref name="T1"/>) without throwing.
    /// </summary>
    /// <param name="value">When this method returns <see langword="true"/>, contains the <typeparamref name="T1"/> value; otherwise the default of <typeparamref name="T1"/>.</param>
    /// <returns><see langword="true"/> if the Variant holds arm <c>1</c>; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Determines whether this Variant equals another Variant of the same arm types. Two Variants are
    /// equal when they share the same discriminator and (for non-NULL values) their held arm values are
    /// equal; two NULL Variants are considered equal.
    /// </summary>
    /// <param name="other">The Variant to compare against.</param>
    /// <returns><see langword="true"/> if the two Variants are equal; otherwise <see langword="false"/>.</returns>
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

    /// <summary>
    /// Determines whether the specified object is a <see cref="VariantValue{T0, T1}"/> equal to this one.
    /// </summary>
    /// <param name="obj">The object to compare against.</param>
    /// <returns><see langword="true"/> if <paramref name="obj"/> is an equal Variant; otherwise <see langword="false"/>.</returns>
    public override bool Equals(object? obj) => obj is VariantValue<T0, T1> v && Equals(v);

    /// <summary>
    /// Returns a hash code derived from the discriminator and the held arm value (or <c>0</c> for NULL).
    /// </summary>
    /// <returns>A hash code consistent with <see cref="Equals(VariantValue{T0, T1})"/>.</returns>
    public override int GetHashCode() => Discriminator switch
    {
        0 => HashCode.Combine((byte)0, _v0),
        1 => HashCode.Combine((byte)1, _v1),
        _ => 0,
    };
}

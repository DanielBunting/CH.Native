namespace CH.Native.Mapping;

/// <summary>
/// Specifies column mapping metadata for a property.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class ClickHouseColumnAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the ClickHouse column name.
    /// If not specified, the property name is used.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Gets or sets the ClickHouse type name.
    /// If not specified, the type is inferred from the property type.
    /// </summary>
    public string? ClickHouseType { get; set; }

    /// <summary>
    /// Gets or sets the column order for INSERT statements.
    /// Lower values are ordered first. Default is int.MaxValue.
    /// </summary>
    public int Order { get; set; } = int.MaxValue;

    /// <summary>
    /// When set to <c>true</c>, the property is excluded from both
    /// bulk-insert column mapping <em>and</em> read-side row mapping —
    /// equivalent to the legacy <c>[Column(Ignore = true)]</c>. Use this
    /// for transient properties (computed values, view-model fields) that
    /// don't correspond to a table column. The property keeps its default
    /// (or constructor-assigned) value on read regardless of whether the
    /// SELECT result includes a column with a matching name.
    /// </summary>
    /// <remarks>
    /// Applying <c>Ignore = true</c> to a property that backs a constructor
    /// parameter (records, immutable POCOs) is rejected at first-row map
    /// time with <see cref="System.InvalidOperationException"/>: an ignored
    /// property cannot also be a required ctor argument.
    /// </remarks>
    public bool Ignore { get; set; }
}

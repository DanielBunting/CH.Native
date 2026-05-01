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
    /// When set to <c>true</c>, the property is excluded from bulk-insert
    /// column mapping entirely — equivalent to the legacy
    /// <c>[Column(Ignore = true)]</c>. Use this for transient properties
    /// (computed values, view-model fields) that don't correspond to a
    /// table column.
    /// </summary>
    public bool Ignore { get; set; }
}

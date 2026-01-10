namespace CH.Native.BulkInsert;

/// <summary>
/// Specifies column mapping metadata for bulk insert operations.
/// </summary>
/// <remarks>
/// This attribute is deprecated. Use <see cref="CH.Native.Mapping.ClickHouseColumnAttribute"/> instead.
/// </remarks>
[Obsolete("Use CH.Native.Mapping.ClickHouseColumnAttribute instead.")]
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class ColumnAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the ClickHouse column name.
    /// If not specified, the property name is used.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Gets or sets the ClickHouse type name.
    /// If not specified, the type is inferred from the schema block.
    /// </summary>
    public string? ClickHouseType { get; set; }

    /// <summary>
    /// Gets or sets the column order for INSERT statements.
    /// Lower values are ordered first. Default is int.MaxValue.
    /// </summary>
    public int Order { get; set; } = int.MaxValue;

    /// <summary>
    /// Gets or sets whether this column should be ignored during bulk insert.
    /// </summary>
    public bool Ignore { get; set; }
}

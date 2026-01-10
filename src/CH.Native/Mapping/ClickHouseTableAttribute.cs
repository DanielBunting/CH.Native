namespace CH.Native.Mapping;

/// <summary>
/// Marks a class or struct for source-generated ClickHouse mapping.
/// The type must be declared as <c>partial</c> for the generator to add the mapper.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
public sealed class ClickHouseTableAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the ClickHouse table name.
    /// If not specified, the type name is used.
    /// </summary>
    public string? TableName { get; set; }
}

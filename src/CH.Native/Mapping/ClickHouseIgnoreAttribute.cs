namespace CH.Native.Mapping;

/// <summary>
/// Marks a property to be excluded from ClickHouse mapping.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class ClickHouseIgnoreAttribute : Attribute
{
}

using Microsoft.CodeAnalysis;

namespace CH.Native.Generators;

/// <summary>
/// Diagnostic descriptors for the ClickHouse mapper generator.
/// </summary>
internal static class DiagnosticDescriptors
{
    private const string Category = "CH.Native.Generators";

    /// <summary>
    /// CHG001: Type must be partial to generate mapper.
    /// </summary>
    public static readonly DiagnosticDescriptor TypeMustBePartial = new(
        id: "CHG001",
        title: "Type must be partial",
        messageFormat: "Type '{0}' must be declared as 'partial' to generate a ClickHouse mapper",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    /// <summary>
    /// CHG002: Unsupported type - use explicit ClickHouseType.
    /// </summary>
    public static readonly DiagnosticDescriptor UnsupportedType = new(
        id: "CHG002",
        title: "Unsupported type",
        messageFormat: "Cannot infer ClickHouse type for property '{0}' of type '{1}'; use [ClickHouseColumn(ClickHouseType = \"...\")] to specify the type explicitly",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    /// <summary>
    /// CHG003: Property has no setter (cannot be used for reading).
    /// </summary>
    public static readonly DiagnosticDescriptor NoSetter = new(
        id: "CHG003",
        title: "Property has no setter",
        messageFormat: "Property '{0}' has no setter and cannot be populated when reading from ClickHouse",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    /// <summary>
    /// CHG004: Property has no getter (cannot be used for writing).
    /// </summary>
    public static readonly DiagnosticDescriptor NoGetter = new(
        id: "CHG004",
        title: "Property has no getter",
        messageFormat: "Property '{0}' has no getter and cannot be used when writing to ClickHouse",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    /// <summary>
    /// CHG005: Duplicate column name.
    /// </summary>
    public static readonly DiagnosticDescriptor DuplicateColumnName = new(
        id: "CHG005",
        title: "Duplicate column name",
        messageFormat: "Column name '{0}' is used by multiple properties",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}

using Microsoft.CodeAnalysis;

namespace CH.Native.Generators;

/// <summary>
/// Helper for mapping CLR types to ClickHouse types at compile time.
/// </summary>
internal static class TypeMappingHelper
{
    /// <summary>
    /// Infers the ClickHouse type from a CLR type symbol.
    /// </summary>
    /// <param name="type">The type symbol to map.</param>
    /// <returns>The ClickHouse type name, or null if unsupported.</returns>
    public static string? GetClickHouseType(ITypeSymbol type)
    {
        // Handle nullable value types
        if (type.NullableAnnotation == NullableAnnotation.Annotated ||
            (type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T && type is INamedTypeSymbol namedNullable))
        {
            var underlyingType = GetUnderlyingType(type);
            if (underlyingType != null)
            {
                var innerType = GetClickHouseType(underlyingType);
                return innerType != null ? $"Nullable({innerType})" : null;
            }
        }

        // Handle arrays
        if (type is IArrayTypeSymbol arrayType)
        {
            var elementType = GetClickHouseType(arrayType.ElementType);
            return elementType != null ? $"Array({elementType})" : null;
        }

        // Handle special types
        switch (type.SpecialType)
        {
            case SpecialType.System_Boolean:
                return "Bool";
            case SpecialType.System_SByte:
                return "Int8";
            case SpecialType.System_Int16:
                return "Int16";
            case SpecialType.System_Int32:
                return "Int32";
            case SpecialType.System_Int64:
                return "Int64";
            case SpecialType.System_Byte:
                return "UInt8";
            case SpecialType.System_UInt16:
                return "UInt16";
            case SpecialType.System_UInt32:
                return "UInt32";
            case SpecialType.System_UInt64:
                return "UInt64";
            case SpecialType.System_Single:
                return "Float32";
            case SpecialType.System_Double:
                return "Float64";
            case SpecialType.System_Decimal:
                return "Decimal128(18)";
            case SpecialType.System_String:
                return "String";
            case SpecialType.System_DateTime:
                return "DateTime";
        }

        // Handle other known types by full name
        var fullName = GetFullTypeName(type);
        return fullName switch
        {
            "System.DateTimeOffset" => "DateTime64(6)",
            "System.DateOnly" => "Date",
            "System.Guid" => "UUID",
            "System.Net.IPAddress" => "IPv6",
            "System.Int128" => "Int128",
            "System.UInt128" => "UInt128",
            _ => null
        };
    }

    /// <summary>
    /// Checks if a type is nullable (either reference type or Nullable&lt;T&gt;).
    /// </summary>
    public static bool IsNullable(ITypeSymbol type)
    {
        // Reference types with nullable annotation
        if (type.IsReferenceType && type.NullableAnnotation == NullableAnnotation.Annotated)
            return true;

        // Nullable<T> value type
        if (type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            return true;

        return false;
    }

    /// <summary>
    /// Gets the underlying type for Nullable&lt;T&gt; or a nullable reference type.
    /// </summary>
    private static ITypeSymbol? GetUnderlyingType(ITypeSymbol type)
    {
        // Handle Nullable<T>
        if (type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T && type is INamedTypeSymbol namedType)
        {
            return namedType.TypeArguments[0];
        }

        // For reference types, return the type itself (the nullability is annotation-based)
        if (type.IsReferenceType && type.NullableAnnotation == NullableAnnotation.Annotated)
        {
            return type.WithNullableAnnotation(NullableAnnotation.NotAnnotated);
        }

        return null;
    }

    /// <summary>
    /// Gets the full type name including namespace.
    /// </summary>
    private static string GetFullTypeName(ITypeSymbol type)
    {
        if (type.ContainingNamespace == null || type.ContainingNamespace.IsGlobalNamespace)
            return type.Name;

        return $"{type.ContainingNamespace.ToDisplayString()}.{type.Name}";
    }

    /// <summary>
    /// Gets the C# type name suitable for code generation.
    /// </summary>
    public static string GetClrTypeName(ITypeSymbol type)
    {
        // Use built-in aliases for common types
        return type.SpecialType switch
        {
            SpecialType.System_Boolean => "bool",
            SpecialType.System_SByte => "sbyte",
            SpecialType.System_Int16 => "short",
            SpecialType.System_Int32 => "int",
            SpecialType.System_Int64 => "long",
            SpecialType.System_Byte => "byte",
            SpecialType.System_UInt16 => "ushort",
            SpecialType.System_UInt32 => "uint",
            SpecialType.System_UInt64 => "ulong",
            SpecialType.System_Single => "float",
            SpecialType.System_Double => "double",
            SpecialType.System_Decimal => "decimal",
            SpecialType.System_String => "string",
            _ => type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
        };
    }
}

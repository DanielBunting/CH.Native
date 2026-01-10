using System.Collections.Frozen;
using CH.Native.Data.ColumnSkippers;

namespace CH.Native.Data;

/// <summary>
/// Registry for column skippers that maps ClickHouse type names to skippers.
/// Used for the scan pass to validate block completeness without allocation.
/// </summary>
public sealed class ColumnSkipperRegistry
{
    private readonly FrozenDictionary<string, IColumnSkipper> _skippers;

    /// <summary>
    /// Gets the default registry with all built-in column skippers.
    /// </summary>
    public static ColumnSkipperRegistry Default { get; } = CreateDefault();

    internal ColumnSkipperRegistry(FrozenDictionary<string, IColumnSkipper> skippers)
    {
        _skippers = skippers;
    }

    /// <summary>
    /// Gets a column skipper for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Int32", "String").</param>
    /// <returns>The column skipper for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnSkipper GetSkipper(string typeName)
    {
        // Try exact match first
        if (_skippers.TryGetValue(typeName, out var skipper))
        {
            return skipper;
        }

        // Handle parameterized types using the factory
        var baseType = ExtractBaseType(typeName);

        // Check if this is a composite type that needs factory handling
        if (IsCompositeType(baseType))
        {
            var factory = new ColumnSkipperFactory(this);
            return factory.CreateSkipper(typeName);
        }

        // For simple parameterized types (e.g., Enum8('foo' = 1)), try base type lookup
        if (baseType != null && _skippers.TryGetValue(baseType, out skipper))
        {
            return skipper;
        }

        throw new NotSupportedException($"Column type '{typeName}' is not supported for skipping.");
    }

    private static string? ExtractBaseType(string typeName)
    {
        // Handle parameterized types like Enum8('foo' = 1), DateTime64(3), etc.
        var parenIndex = typeName.IndexOf('(');
        if (parenIndex > 0)
        {
            return typeName.Substring(0, parenIndex);
        }
        return null;
    }

    private static bool IsCompositeType(string? baseType)
    {
        return baseType is "Nullable" or "Array" or "Map" or "Tuple" or "LowCardinality"
            or "FixedString" or "DateTime" or "DateTime64" or "Nested"
            or "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256";
    }

    /// <summary>
    /// Tries to get a column skipper for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name.</param>
    /// <param name="skipper">The column skipper if found.</param>
    /// <returns>True if a skipper was found, false otherwise.</returns>
    public bool TryGetSkipper(string typeName, out IColumnSkipper? skipper)
    {
        return _skippers.TryGetValue(typeName, out skipper);
    }

    private static ColumnSkipperRegistry CreateDefault()
    {
        var builder = new ColumnSkipperRegistryBuilder();
        builder.RegisterDefaults();
        return builder.Build();
    }
}

/// <summary>
/// Builder for creating customized column skipper registries.
/// </summary>
public sealed class ColumnSkipperRegistryBuilder
{
    private readonly Dictionary<string, IColumnSkipper> _skippers = new();

    /// <summary>
    /// Registers a column skipper for a type name.
    /// </summary>
    /// <param name="skipper">The column skipper to register.</param>
    /// <returns>This builder for chaining.</returns>
    public ColumnSkipperRegistryBuilder Register(IColumnSkipper skipper)
    {
        _skippers[skipper.TypeName] = skipper;
        return this;
    }

    /// <summary>
    /// Registers all default column skippers.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public ColumnSkipperRegistryBuilder RegisterDefaults()
    {
        // 1-byte types
        Register(new Int8ColumnSkipper());
        Register(new UInt8ColumnSkipper());
        Register(new BoolColumnSkipper());

        // 2-byte types
        Register(new Int16ColumnSkipper());
        Register(new UInt16ColumnSkipper());
        Register(new DateColumnSkipper());

        // 4-byte types
        Register(new Int32ColumnSkipper());
        Register(new UInt32ColumnSkipper());
        Register(new Float32ColumnSkipper());
        Register(new DateTimeColumnSkipper());
        Register(new Date32ColumnSkipper());
        Register(new IPv4ColumnSkipper());
        Register(new Decimal32ColumnSkipper());

        // 8-byte types
        Register(new Int64ColumnSkipper());
        Register(new UInt64ColumnSkipper());
        Register(new Float64ColumnSkipper());
        Register(new DateTime64ColumnSkipper());
        Register(new Decimal64ColumnSkipper());

        // 16-byte types
        Register(new Int128ColumnSkipper());
        Register(new UInt128ColumnSkipper());
        Register(new UuidColumnSkipper());
        Register(new IPv6ColumnSkipper());
        Register(new Decimal128ColumnSkipper());

        // 32-byte types
        Register(new Int256ColumnSkipper());
        Register(new UInt256ColumnSkipper());
        Register(new Decimal256ColumnSkipper());

        // Enum types
        Register(new Enum8ColumnSkipper());
        Register(new Enum16ColumnSkipper());

        // String type
        Register(new StringColumnSkipper());

        return this;
    }

    /// <summary>
    /// Builds the column skipper registry.
    /// </summary>
    /// <returns>The built registry.</returns>
    public ColumnSkipperRegistry Build()
    {
        return new ColumnSkipperRegistry(_skippers.ToFrozenDictionary());
    }
}

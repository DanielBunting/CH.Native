using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, IColumnSkipper> _parameterizedCache = new();

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
        // Try exact match first (simple types)
        if (_skippers.TryGetValue(typeName, out var skipper))
        {
            return skipper;
        }

        // Check parameterized cache (composite types like Array(Int32), Nullable(String))
        if (_parameterizedCache.TryGetValue(typeName, out skipper))
        {
            return skipper;
        }

        // Handle parameterized types using the factory with caching
        var baseType = ExtractBaseType(typeName);

        // Check if this is a composite type that needs factory handling
        if (IsCompositeType(baseType))
        {
            // Use GetOrAdd to ensure thread-safe caching of parameterized type skippers
            return _parameterizedCache.GetOrAdd(typeName, static (key, registry) =>
            {
                var factory = new ColumnSkipperFactory(registry);
                return factory.CreateSkipper(key);
            }, this);
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
            or "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256"
            or "JSON";
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

    /// <summary>
    /// Tries to get a column skipper by comparing UTF-8 bytes directly.
    /// This avoids string allocation during the scan pass for common types.
    /// </summary>
    /// <param name="typeNameUtf8">The UTF-8 encoded type name bytes.</param>
    /// <returns>The skipper if found, null if the type requires string-based lookup.</returns>
    public IColumnSkipper? TryGetSkipperByBytes(ReadOnlySpan<byte> typeNameUtf8)
    {
        // Fast path: check common simple types using byte comparison
        // These cover the most frequently encountered column types

        // Most common: 8-byte and 4-byte numeric types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int64))
            return _skippers["Int64"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int32))
            return _skippers["Int32"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt64))
            return _skippers["UInt64"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt32))
            return _skippers["UInt32"];

        // String is very common
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.String))
            return _skippers["String"];

        // Float types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Float64))
            return _skippers["Float64"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Float32))
            return _skippers["Float32"];

        // Date/time types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.DateTime))
            return _skippers["DateTime"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Date))
            return _skippers["Date"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Date32))
            return _skippers["Date32"];

        // Smaller numeric types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int16))
            return _skippers["Int16"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt16))
            return _skippers["UInt16"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int8))
            return _skippers["Int8"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt8))
            return _skippers["UInt8"];

        // Large numeric types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int128))
            return _skippers["Int128"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt128))
            return _skippers["UInt128"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Int256))
            return _skippers["Int256"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UInt256))
            return _skippers["UInt256"];

        // UUID and IP types
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.UUID))
            return _skippers["UUID"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.IPv4))
            return _skippers["IPv4"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.IPv6))
            return _skippers["IPv6"];

        // Bool
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Bool))
            return _skippers["Bool"];

        // Enum types (base types - parameterized enums fall through to string path)
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Enum8))
            return _skippers["Enum8"];
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.Enum16))
            return _skippers["Enum16"];

        // JSON type
        if (typeNameUtf8.SequenceEqual(Utf8TypeNames.JSON))
            return _skippers["JSON"];

        // Not a simple type - return null to indicate string-based lookup needed
        // (for parameterized types like Array(T), Nullable(T), etc.)
        return null;
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

        // JSON type
        Register(new JsonColumnSkipper());

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

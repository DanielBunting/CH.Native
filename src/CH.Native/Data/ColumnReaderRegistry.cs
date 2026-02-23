using System.Collections.Frozen;

namespace CH.Native.Data;

/// <summary>
/// Registry for column readers that maps ClickHouse type names to readers.
/// </summary>
public sealed class ColumnReaderRegistry
{
    private readonly FrozenDictionary<string, IColumnReader> _readers;

    /// <summary>
    /// Gets the default registry with all built-in column readers (eager string materialization).
    /// </summary>
    public static ColumnReaderRegistry Default { get; } = CreateDefault();

    /// <summary>
    /// Gets the registry with lazy string materialization.
    /// </summary>
    public static ColumnReaderRegistry LazyStrings { get; } = CreateLazyStrings();

    /// <summary>
    /// Gets the string materialization strategy for this registry.
    /// </summary>
    internal StringMaterialization Strategy { get; }

    internal ColumnReaderRegistry(FrozenDictionary<string, IColumnReader> readers, StringMaterialization strategy = StringMaterialization.Eager)
    {
        _readers = readers;
        Strategy = strategy;
    }

    /// <summary>
    /// Gets a column reader for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Int32", "String").</param>
    /// <returns>The column reader for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnReader GetReader(string typeName)
    {
        // Try exact match first
        if (_readers.TryGetValue(typeName, out var reader))
        {
            return reader;
        }

        // Handle parameterized types using the factory
        var baseType = ExtractBaseType(typeName);

        // Check if this is a composite type that needs factory handling
        if (IsCompositeType(baseType))
        {
            var factory = new ColumnReaderFactory(this);
            return factory.CreateReader(typeName);
        }

        // For simple parameterized types (e.g., Enum8('foo' = 1)), try base type lookup
        if (baseType != null && _readers.TryGetValue(baseType, out reader))
        {
            return reader;
        }

        throw new NotSupportedException($"Column type '{typeName}' is not supported.");
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
        return baseType is "Nullable" or "Array" or "Map" or "Tuple" or "LowCardinality" or "Nested"
            or "FixedString" or "DateTime" or "DateTime64"
            or "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256"
            or "JSON";
    }

    /// <summary>
    /// Tries to get a column reader for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name.</param>
    /// <param name="reader">The column reader if found.</param>
    /// <returns>True if a reader was found, false otherwise.</returns>
    public bool TryGetReader(string typeName, out IColumnReader? reader)
    {
        return _readers.TryGetValue(typeName, out reader);
    }

    /// <summary>
    /// Creates a new registry builder for customizing readers.
    /// </summary>
    /// <returns>A new registry builder.</returns>
    public static ColumnReaderRegistryBuilder CreateBuilder()
    {
        return new ColumnReaderRegistryBuilder();
    }

    private static ColumnReaderRegistry CreateDefault()
    {
        var builder = new ColumnReaderRegistryBuilder();
        builder.RegisterDefaults();
        return builder.Build();
    }

    private static ColumnReaderRegistry CreateLazyStrings()
    {
        var builder = new ColumnReaderRegistryBuilder();
        builder.RegisterDefaults();
        builder.Register(new ColumnReaders.StringColumnReader(lazy: true));
        return builder.Build(StringMaterialization.Lazy);
    }
}

/// <summary>
/// Builder for creating customized column reader registries.
/// </summary>
public sealed class ColumnReaderRegistryBuilder
{
    private readonly Dictionary<string, IColumnReader> _readers = new();

    /// <summary>
    /// Registers a column reader for a type name.
    /// </summary>
    /// <param name="reader">The column reader to register.</param>
    /// <returns>This builder for chaining.</returns>
    public ColumnReaderRegistryBuilder Register(IColumnReader reader)
    {
        _readers[reader.TypeName] = reader;
        return this;
    }

    /// <summary>
    /// Registers all default column readers.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public ColumnReaderRegistryBuilder RegisterDefaults()
    {
        // Integer types
        Register(new ColumnReaders.Int8ColumnReader());
        Register(new ColumnReaders.Int16ColumnReader());
        Register(new ColumnReaders.Int32ColumnReader());
        Register(new ColumnReaders.Int64ColumnReader());
        Register(new ColumnReaders.UInt8ColumnReader());
        Register(new ColumnReaders.UInt16ColumnReader());
        Register(new ColumnReaders.UInt32ColumnReader());
        Register(new ColumnReaders.UInt64ColumnReader());

        // 128-bit integer types
        Register(new ColumnReaders.Int128ColumnReader());
        Register(new ColumnReaders.UInt128ColumnReader());

        // 256-bit integer types
        Register(new ColumnReaders.Int256ColumnReader());
        Register(new ColumnReaders.UInt256ColumnReader());

        // Floating point types
        Register(new ColumnReaders.Float32ColumnReader());
        Register(new ColumnReaders.Float64ColumnReader());

        // Boolean type
        Register(new ColumnReaders.BoolColumnReader());

        // String type
        Register(new ColumnReaders.StringColumnReader());

        // Date/Time types
        Register(new ColumnReaders.DateTimeColumnReader());
        Register(new ColumnReaders.DateColumnReader());
        Register(new ColumnReaders.Date32ColumnReader());

        // UUID type
        Register(new ColumnReaders.UuidColumnReader());

        // IP address types
        Register(new ColumnReaders.IPv4ColumnReader());
        Register(new ColumnReaders.IPv6ColumnReader());

        // Enum types
        Register(new ColumnReaders.Enum8ColumnReader());
        Register(new ColumnReaders.Enum16ColumnReader());

        // JSON type
        Register(new ColumnReaders.JsonColumnReader());

        return this;
    }

    /// <summary>
    /// Builds the column reader registry.
    /// </summary>
    /// <param name="strategy">The string materialization strategy for this registry.</param>
    /// <returns>The built registry.</returns>
    public ColumnReaderRegistry Build(StringMaterialization strategy = StringMaterialization.Eager)
    {
        return new ColumnReaderRegistry(_readers.ToFrozenDictionary(), strategy);
    }
}

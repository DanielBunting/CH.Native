using System.Collections.Frozen;

namespace CH.Native.Data;

/// <summary>
/// Registry for column writers that maps ClickHouse type names to writers.
/// </summary>
public sealed class ColumnWriterRegistry
{
    private readonly FrozenDictionary<string, IColumnWriter> _writers;

    /// <summary>
    /// Gets the default registry with all built-in column writers.
    /// </summary>
    public static ColumnWriterRegistry Default { get; } = CreateDefault();

    internal ColumnWriterRegistry(FrozenDictionary<string, IColumnWriter> writers)
    {
        _writers = writers;
    }

    /// <summary>
    /// Gets a column writer for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name (e.g., "Int32", "String").</param>
    /// <returns>The column writer for the type.</returns>
    /// <exception cref="NotSupportedException">Thrown if the type is not supported.</exception>
    public IColumnWriter GetWriter(string typeName)
    {
        // Try exact match first
        if (_writers.TryGetValue(typeName, out var writer))
        {
            return writer;
        }

        // Handle parameterized types using the factory
        var baseType = ExtractBaseType(typeName);

        // Check if this is a composite type that needs factory handling
        if (IsCompositeType(baseType))
        {
            var factory = new ColumnWriterFactory(this);
            return factory.CreateWriter(typeName);
        }

        // For simple parameterized types (e.g., Enum8('foo' = 1)), try base type lookup
        if (baseType != null && _writers.TryGetValue(baseType, out writer))
        {
            return writer;
        }

        throw new NotSupportedException($"Column type '{typeName}' is not supported for writing.");
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
            or "Decimal" or "Decimal32" or "Decimal64" or "Decimal128" or "Decimal256";
    }

    /// <summary>
    /// Tries to get a column writer for the specified ClickHouse type name.
    /// </summary>
    /// <param name="typeName">The ClickHouse type name.</param>
    /// <param name="writer">The column writer if found.</param>
    /// <returns>True if a writer was found, false otherwise.</returns>
    public bool TryGetWriter(string typeName, out IColumnWriter? writer)
    {
        return _writers.TryGetValue(typeName, out writer);
    }

    /// <summary>
    /// Creates a new registry builder for customizing writers.
    /// </summary>
    /// <returns>A new registry builder.</returns>
    public static ColumnWriterRegistryBuilder CreateBuilder()
    {
        return new ColumnWriterRegistryBuilder();
    }

    private static ColumnWriterRegistry CreateDefault()
    {
        var builder = new ColumnWriterRegistryBuilder();
        builder.RegisterDefaults();
        return builder.Build();
    }
}

/// <summary>
/// Builder for creating customized column writer registries.
/// </summary>
public sealed class ColumnWriterRegistryBuilder
{
    private readonly Dictionary<string, IColumnWriter> _writers = new();

    /// <summary>
    /// Registers a column writer for a type name.
    /// </summary>
    /// <param name="writer">The column writer to register.</param>
    /// <returns>This builder for chaining.</returns>
    public ColumnWriterRegistryBuilder Register(IColumnWriter writer)
    {
        _writers[writer.TypeName] = writer;
        return this;
    }

    /// <summary>
    /// Registers all default column writers.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public ColumnWriterRegistryBuilder RegisterDefaults()
    {
        // Integer types
        Register(new ColumnWriters.Int8ColumnWriter());
        Register(new ColumnWriters.Int16ColumnWriter());
        Register(new ColumnWriters.Int32ColumnWriter());
        Register(new ColumnWriters.Int64ColumnWriter());
        Register(new ColumnWriters.UInt8ColumnWriter());
        Register(new ColumnWriters.UInt16ColumnWriter());
        Register(new ColumnWriters.UInt32ColumnWriter());
        Register(new ColumnWriters.UInt64ColumnWriter());

        // 128-bit integer types
        Register(new ColumnWriters.Int128ColumnWriter());
        Register(new ColumnWriters.UInt128ColumnWriter());

        // 256-bit integer types
        Register(new ColumnWriters.Int256ColumnWriter());
        Register(new ColumnWriters.UInt256ColumnWriter());

        // Floating point types
        Register(new ColumnWriters.Float32ColumnWriter());
        Register(new ColumnWriters.Float64ColumnWriter());

        // Boolean type
        Register(new ColumnWriters.BoolColumnWriter());

        // String type
        Register(new ColumnWriters.StringColumnWriter());

        // Date/Time types
        Register(new ColumnWriters.DateTimeColumnWriter());
        Register(new ColumnWriters.DateColumnWriter());
        Register(new ColumnWriters.Date32ColumnWriter());

        // UUID type
        Register(new ColumnWriters.UuidColumnWriter());

        // IP address types
        Register(new ColumnWriters.IPv4ColumnWriter());
        Register(new ColumnWriters.IPv6ColumnWriter());

        // Enum types
        Register(new ColumnWriters.Enum8ColumnWriter());
        Register(new ColumnWriters.Enum16ColumnWriter());

        return this;
    }

    /// <summary>
    /// Builds the column writer registry.
    /// </summary>
    /// <returns>The built registry.</returns>
    public ColumnWriterRegistry Build()
    {
        return new ColumnWriterRegistry(_writers.ToFrozenDictionary());
    }
}

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using CH.Native.Data;

namespace CH.Native.Mapping;

/// <summary>
/// Walks a typed row class's properties to determine which CLR-side shape each
/// <c>Map(K, V)</c> column should be materialised as. Only the top-level property
/// type is inspected; nested generic wrappers (e.g.,
/// <c>List&lt;ClickHouseMap&lt;K, V&gt;&gt;</c>) fall through to the connection
/// default.
/// </summary>
internal static class MapShapeInspector
{
    private static readonly ConcurrentDictionary<Type, IReadOnlyDictionary<string, MapShape>> _perTypeCache = new();
    private static readonly ConcurrentDictionary<Type, MapShape> _scalarCache = new();

    // Shared sentinel for "no Map-shape properties on this type" — avoids an
    // empty-Dictionary allocation per call on the common path.
    internal static readonly IReadOnlyDictionary<string, MapShape> Empty =
        new Dictionary<string, MapShape>(0, StringComparer.Ordinal);

    /// <summary>
    /// Inspects the supplied row type and returns a column-name → <see cref="MapShape"/>
    /// dictionary suitable for constructing a <see cref="MapShapeHint"/>.
    /// Properties typed as anything other than a recognised Map shape are omitted.
    /// Results are cached per <paramref name="rowType"/>.
    /// </summary>
    public static IReadOnlyDictionary<string, MapShape> Inspect(Type rowType)
        => _perTypeCache.GetOrAdd(rowType, static t => BuildHints(t));

    private static IReadOnlyDictionary<string, MapShape> BuildHints(Type rowType)
    {
        Dictionary<string, MapShape>? hints = null;

        foreach (var property in rowType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead && !property.CanWrite) continue;

            var shape = ClassifyTopLevel(property.PropertyType);
            if (shape == MapShape.Default) continue;

            hints ??= new Dictionary<string, MapShape>(StringComparer.Ordinal);
            hints[ResolveColumnName(property)] = shape;
        }

        return hints ?? Empty;
    }

    /// <summary>
    /// Inspects a scalar return type. Returns <see cref="MapShape.Entries"/> for
    /// <see cref="ClickHouseMap{TKey, TValue}"/> / <c>KeyValuePair&lt;K, V&gt;[]</c> /
    /// <c>IReadOnlyList&lt;KeyValuePair&lt;K, V&gt;&gt;</c>; <see cref="MapShape.Dictionary"/>
    /// for <c>Dictionary&lt;K, V&gt;</c> / <c>IDictionary&lt;K, V&gt;</c> /
    /// <c>IReadOnlyDictionary&lt;K, V&gt;</c>; <see cref="MapShape.Default"/> otherwise.
    /// Results are cached per <paramref name="targetType"/>.
    /// </summary>
    public static MapShape InspectScalar(Type targetType)
        => _scalarCache.GetOrAdd(targetType, static t => ClassifyTopLevel(Nullable.GetUnderlyingType(t) ?? t));

    private static MapShape ClassifyTopLevel(Type type)
    {
        // KeyValuePair<K, V>[]
        if (type.IsArray)
        {
            var element = type.GetElementType();
            if (element is not null
                && element.IsGenericType
                && element.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
            {
                return MapShape.Entries;
            }
            return MapShape.Default;
        }

        if (!type.IsGenericType) return MapShape.Default;

        var definition = type.IsGenericTypeDefinition ? type : type.GetGenericTypeDefinition();

        if (definition == typeof(Dictionary<,>)
            || definition == typeof(IDictionary<,>)
            || definition == typeof(IReadOnlyDictionary<,>))
        {
            return MapShape.Dictionary;
        }

        if (definition == typeof(ClickHouseMap<,>))
        {
            return MapShape.Entries;
        }

        // KeyValuePair<,>[] is an array, handled below as a special case.

        // Sequences-of-KeyValuePair shapes
        if (IsKeyValuePairSequence(type))
        {
            return MapShape.Entries;
        }

        return MapShape.Default;
    }

    private static bool IsKeyValuePairSequence(Type type)
    {
        if (!type.IsGenericType) return false;
        var definition = type.GetGenericTypeDefinition();
        if (definition != typeof(IReadOnlyList<>)
            && definition != typeof(IList<>)
            && definition != typeof(IEnumerable<>)
            && definition != typeof(ICollection<>)
            && definition != typeof(IReadOnlyCollection<>)
            && definition != typeof(List<>))
        {
            return false;
        }

        var elementType = type.GetGenericArguments()[0];
        return elementType.IsGenericType
            && elementType.GetGenericTypeDefinition() == typeof(KeyValuePair<,>);
    }

    private static string ResolveColumnName(PropertyInfo property)
    {
        var attr = property.GetCustomAttribute<ClickHouseColumnAttribute>();
        return attr?.Name ?? property.Name;
    }
}

using System.Collections.Concurrent;

namespace CH.Native.Mapping;

/// <summary>
/// Factory for creating typed row mappers.
/// Caches mappers for reuse.
/// </summary>
public static class TypedRowMapperFactory
{
    private static readonly ConcurrentDictionary<(Type, string), object> _mapperCache = new();

    /// <summary>
    /// Creates or retrieves a cached mapper for the given type and column names.
    /// </summary>
    /// <typeparam name="T">The POCO type to map to.</typeparam>
    /// <param name="columnNames">The column names in order.</param>
    /// <returns>A typed row mapper.</returns>
    public static ITypedRowMapper<T> GetMapper<T>(string[] columnNames) where T : new()
    {
        var key = (typeof(T), string.Join(",", columnNames));

        return (ITypedRowMapper<T>)_mapperCache.GetOrAdd(key, _ => new ReflectionTypedRowMapper<T>(columnNames));
    }
}

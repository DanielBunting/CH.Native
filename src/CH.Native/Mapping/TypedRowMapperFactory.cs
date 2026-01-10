using System.Collections.Concurrent;
using System.Reflection;
using CH.Native.Data;

namespace CH.Native.Mapping;

/// <summary>
/// Factory for creating typed row mappers.
/// Caches mappers for reuse and checks for source-generated mappers first.
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

        return (ITypedRowMapper<T>)_mapperCache.GetOrAdd(key, _ => CreateMapper<T>(columnNames));
    }

    private static ITypedRowMapper<T> CreateMapper<T>(string[] columnNames) where T : new()
    {
        // Try to find a source-generated mapper first
        var generatedMapper = TryGetGeneratedMapper<T>(columnNames);
        if (generatedMapper != null)
        {
            return generatedMapper;
        }

        // Fall back to reflection-based mapper
        return new ReflectionTypedRowMapper<T>(columnNames);
    }

    private static ITypedRowMapper<T>? TryGetGeneratedMapper<T>(string[] columnNames) where T : new()
    {
        // Look for a nested ClickHouseMapper class with a MapTypedRow method
        var type = typeof(T);
        var mapperType = type.GetNestedType("ClickHouseMapper", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

        if (mapperType == null)
            return null;

        // Look for MapTypedRow(ITypedColumn[] columns, int rowIndex) method
        var mapTypedRowMethod = mapperType.GetMethod("MapTypedRow", BindingFlags.Public | BindingFlags.Static);
        if (mapTypedRowMethod == null ||
            mapTypedRowMethod.ReturnType != typeof(T) ||
            mapTypedRowMethod.GetParameters().Length != 2 ||
            mapTypedRowMethod.GetParameters()[0].ParameterType != typeof(ITypedColumn[]) ||
            mapTypedRowMethod.GetParameters()[1].ParameterType != typeof(int))
        {
            return null;
        }

        // Create a wrapper that calls the generated method
        var mapFunc = (Func<ITypedColumn[], int, T>)Delegate.CreateDelegate(
            typeof(Func<ITypedColumn[], int, T>),
            mapTypedRowMethod);

        return new GeneratedTypedRowMapper<T>(mapFunc, columnNames);
    }
}

/// <summary>
/// Wrapper for source-generated mappers.
/// </summary>
internal sealed class GeneratedTypedRowMapper<T> : ITypedRowMapper<T>
{
    private readonly Func<ITypedColumn[], int, T> _mapFunc;
    private readonly int[] _columnIndexMap;

    public GeneratedTypedRowMapper(Func<ITypedColumn[], int, T> mapFunc, string[] columnNames)
    {
        _mapFunc = mapFunc;

        // Get expected column names from the generated mapper
        if (GeneratedMapperHelper.TryGetColumnNames<T>(out var expectedColumnNames) && expectedColumnNames != null)
        {
            // Build index map: expectedColumnNames[i] -> actual columnNames index
            _columnIndexMap = new int[expectedColumnNames.Length];
            for (int i = 0; i < expectedColumnNames.Length; i++)
            {
                _columnIndexMap[i] = Array.FindIndex(columnNames,
                    name => name.Equals(expectedColumnNames[i], StringComparison.OrdinalIgnoreCase));
            }
        }
        else
        {
            // No column name mapping needed - assume 1:1
            _columnIndexMap = Enumerable.Range(0, columnNames.Length).ToArray();
        }
    }

    public T MapRow(ITypedColumn[] columns, int rowIndex)
    {
        // Reorder columns if needed based on index map
        // For now, pass through directly (assumes column order matches)
        return _mapFunc(columns, rowIndex);
    }
}

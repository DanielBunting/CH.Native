using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Data;
using CH.Native.Numerics;

namespace CH.Native.Mapping;

/// <summary>
/// Reflection-based row mapper for typed columns.
/// Uses compiled expressions for performance after initial setup.
/// </summary>
/// <typeparam name="T">The POCO type to map to.</typeparam>
public sealed class ReflectionTypedRowMapper<T> : ITypedRowMapper<T> where T : new()
{
    private readonly Action<T, ITypedColumn[], int>[] _setters;

    /// <summary>
    /// Creates a new reflection-based mapper for the given column names.
    /// </summary>
    /// <param name="columnNames">The column names in order.</param>
    public ReflectionTypedRowMapper(string[] columnNames)
    {
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .ToDictionary(p => GetColumnName(p), StringComparer.OrdinalIgnoreCase);

        _setters = new Action<T, ITypedColumn[], int>[columnNames.Length];

        for (int i = 0; i < columnNames.Length; i++)
        {
            if (properties.TryGetValue(columnNames[i], out var property))
            {
                _setters[i] = CreateSetter(property, i);
            }
            else
            {
                // No matching property - create no-op setter
                _setters[i] = static (_, _, _) => { };
            }
        }
    }

    /// <inheritdoc />
    public T MapRow(ITypedColumn[] columns, int rowIndex)
    {
        var result = new T();
        for (int i = 0; i < _setters.Length; i++)
        {
            _setters[i](result, columns, rowIndex);
        }
        return result;
    }

    private static string GetColumnName(PropertyInfo property)
    {
        var attr = property.GetCustomAttribute<ClickHouseColumnAttribute>();
        return attr?.Name ?? property.Name;
    }

    private static Action<T, ITypedColumn[], int> CreateSetter(PropertyInfo property, int columnIndex)
    {
        var propertyType = property.PropertyType;

        // Create compiled expression for fast property setting
        var targetParam = Expression.Parameter(typeof(T), "target");
        var columnsParam = Expression.Parameter(typeof(ITypedColumn[]), "columns");
        var rowIndexParam = Expression.Parameter(typeof(int), "rowIndex");

        // Get the column: columns[columnIndex]
        var columnAccess = Expression.ArrayIndex(columnsParam, Expression.Constant(columnIndex));

        // Cast to TypedColumn<PropertyType>
        var typedColumnType = typeof(TypedColumn<>).MakeGenericType(propertyType);

        Expression valueExpression;

        // Check if the column type matches directly
        if (IsDirectlyMappable(propertyType))
        {
            // Direct cast: ((TypedColumn<T>)column)[rowIndex]
            var castColumn = Expression.Convert(columnAccess, typedColumnType);
            var indexer = typedColumnType.GetProperty("Item")!;
            valueExpression = Expression.MakeIndex(castColumn, indexer, new[] { rowIndexParam });
        }
        else
        {
            // Fallback to GetValue with boxing: column.GetValue(rowIndex)
            var getValueMethod = typeof(ITypedColumn).GetMethod(nameof(ITypedColumn.GetValue))!;
            var boxedValue = Expression.Call(columnAccess, getValueMethod, rowIndexParam);
            valueExpression = Expression.Convert(boxedValue, propertyType);
        }

        // Set property: target.Property = value
        var propertyAccess = Expression.Property(targetParam, property);
        var assignment = Expression.Assign(propertyAccess, valueExpression);

        var lambda = Expression.Lambda<Action<T, ITypedColumn[], int>>(
            assignment, targetParam, columnsParam, rowIndexParam);

        return lambda.Compile();
    }

    private static bool IsDirectlyMappable(Type type)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType == typeof(bool) ||
               underlyingType == typeof(byte) ||
               underlyingType == typeof(sbyte) ||
               underlyingType == typeof(short) ||
               underlyingType == typeof(ushort) ||
               underlyingType == typeof(int) ||
               underlyingType == typeof(uint) ||
               underlyingType == typeof(long) ||
               underlyingType == typeof(ulong) ||
               underlyingType == typeof(float) ||
               underlyingType == typeof(double) ||
               underlyingType == typeof(decimal) ||
               underlyingType == typeof(DateTime) ||
               underlyingType == typeof(DateTimeOffset) ||
               underlyingType == typeof(Guid) ||
               underlyingType == typeof(string) ||
               underlyingType == typeof(ClickHouseDecimal);
    }
}

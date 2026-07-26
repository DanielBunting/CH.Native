using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Data;
using CH.Native.Data.Conversion;
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
    /// Set when T is a scalar rather than a POCO; non-null means the per-property
    /// setters are bypassed and the row's first column becomes the value.
    /// </summary>
    private readonly Func<ITypedColumn[], int, T>? _scalarMap;

    /// <summary>
    /// Creates a new reflection-based mapper for the given column names.
    /// </summary>
    /// <param name="columnNames">The column names in order.</param>
    public ReflectionTypedRowMapper(string[] columnNames)
    {
        // Scalar T (int, ulong, Guid, DateTime, an enum, or a Nullable of one)
        // takes the row's first column, matching ExecuteScalarAsync. Without this
        // branch a scalar contributes no writable properties, every setter below
        // is the no-op, and MapRow hands back `new T()` — default(T) for every
        // row, with the decoded column value silently discarded.
        if (IsScalarTarget(typeof(T)))
        {
            _scalarMap = BuildScalarMap();
            _setters = Array.Empty<Action<T, ITypedColumn[], int>>();
            return;
        }

        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            // [ClickHouseColumn(Ignore = true)] excludes the property from
            // read-side mapping — same contract as the bulk-insert side and
            // the slow-path TypeMapper. Without this, the typed fast-path
            // would silently populate properties the user explicitly opted
            // out of.
            .Where(p => p.GetCustomAttribute<ClickHouseColumnAttribute>()?.Ignore != true)
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
        if (_scalarMap is not null)
            return _scalarMap(columns, rowIndex);

        var result = new T();
        for (int i = 0; i < _setters.Length; i++)
        {
            _setters[i](result, columns, rowIndex);
        }
        return result;
    }

    /// <summary>
    /// True when T is a single column's value rather than a POCO assembled from
    /// columns. Kept in step with <see cref="IsDirectlyMappable"/>, plus enums,
    /// which arrive as their numeric or string representation.
    /// </summary>
    private static bool IsScalarTarget(Type type)
    {
        if (IsDirectlyMappable(type))
            return true;

        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsEnum
            || underlying == typeof(DateOnly)
            || underlying == typeof(TimeOnly);
    }

    /// <summary>
    /// Builds the first-column reader for a scalar T. Goes through the boxed
    /// <see cref="ITypedColumn.GetValue"/> rather than a direct
    /// <c>TypedColumn&lt;T&gt;</c> cast: the column's element type need not match T
    /// exactly (a UInt8 column read as <c>int</c>, an enum read as its underlying
    /// integer), and a hard cast would throw where a conversion is what's wanted.
    /// A SQL NULL yields <c>default(T)</c>, matching the property-setter path.
    /// </summary>
    private static Func<ITypedColumn[], int, T> BuildScalarMap()
    {
        var underlying = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        return (columns, rowIndex) =>
        {
            if (columns.Length == 0)
                throw new InvalidOperationException(
                    $"Cannot map a scalar '{typeof(T).Name}' from a row with no columns.");

            var value = columns[0].GetValue(rowIndex);
            if (value is null)
                return default!;

            // Exact match is the common case (Int32 column → int) — no conversion.
            if (value.GetType() == underlying)
                return (T)value;

            if (underlying.IsEnum)
                return (T)Enum.ToObject(underlying, value);

            return (T)Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
        };
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

            // Rectangular multidim property (T[,], T[,,], …): the column reader
            // returns jagged form (T[][], T[][][]). Cast → throw without the
            // boundary converter, so route through JaggedToRectangularConverter
            // before assignment.
            if (propertyType.IsArray && propertyType.GetArrayRank() > 1)
            {
                var toRect = typeof(JaggedToRectangularConverter)
                    .GetMethod(nameof(JaggedToRectangularConverter.ToRectangular))!;
                var castArray = Expression.Convert(boxedValue, typeof(Array));
                var rectArray = Expression.Call(toRect, castArray, Expression.Constant(propertyType));
                valueExpression = Expression.Convert(rectArray, propertyType);
            }
            else
            {
                valueExpression = Expression.Convert(boxedValue, propertyType);
            }
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

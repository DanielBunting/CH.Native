using System.Linq.Expressions;
using System.Reflection;

namespace CH.Native.Results;

/// <summary>
/// Maps rows from a ClickHouseDataReader to instances of type T using reflection.
/// </summary>
/// <typeparam name="T">The target type to map to.</typeparam>
internal sealed class TypeMapper<T> where T : new()
{
    private readonly PropertyMap[] _propertyMaps;

    private readonly struct PropertyMap
    {
        public required int Ordinal { get; init; }
        public required Action<T, object?> Setter { get; init; }
        public required Type PropertyType { get; init; }
    }

    /// <summary>
    /// Creates a new TypeMapper for the specified reader schema.
    /// </summary>
    /// <param name="reader">The data reader to map from.</param>
    public TypeMapper(ClickHouseDataReader reader)
    {
        _propertyMaps = BuildPropertyMaps(reader);
    }

    /// <summary>
    /// Maps the current row of the reader to a new instance of T.
    /// </summary>
    /// <param name="reader">The data reader positioned on a row.</param>
    /// <returns>A new instance of T with mapped values.</returns>
    public T Map(ClickHouseDataReader reader)
    {
        var instance = new T();

        foreach (ref readonly var map in _propertyMaps.AsSpan())
        {
            var value = reader.GetValue(map.Ordinal);
            var convertedValue = ConvertValue(value, map.PropertyType);
            map.Setter(instance, convertedValue);
        }

        return instance;
    }

    private static PropertyMap[] BuildPropertyMaps(ClickHouseDataReader reader)
    {
        var properties = typeof(T).GetProperties(
            BindingFlags.Public | BindingFlags.Instance);

        var maps = new List<PropertyMap>();

        foreach (var prop in properties)
        {
            if (!prop.CanWrite)
                continue;

            var ordinal = TryGetOrdinal(reader, prop.Name);
            if (ordinal < 0)
                continue;

            maps.Add(new PropertyMap
            {
                Ordinal = ordinal,
                Setter = CreateSetter(prop),
                PropertyType = prop.PropertyType
            });
        }

        return maps.ToArray();
    }

    private static int TryGetOrdinal(ClickHouseDataReader reader, string propertyName)
    {
        try
        {
            return reader.GetOrdinal(propertyName);
        }
        catch (ArgumentException)
        {
            // Try snake_case version of the property name
            var snakeCase = ToSnakeCase(propertyName);
            if (snakeCase != propertyName)
            {
                try
                {
                    return reader.GetOrdinal(snakeCase);
                }
                catch (ArgumentException)
                {
                    // Fall through to return -1
                }
            }
            return -1;
        }
    }

    private static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new System.Text.StringBuilder();
        result.Append(char.ToLowerInvariant(name[0]));

        for (int i = 1; i < name.Length; i++)
        {
            if (char.IsUpper(name[i]))
            {
                result.Append('_');
                result.Append(char.ToLowerInvariant(name[i]));
            }
            else
            {
                result.Append(name[i]);
            }
        }

        return result.ToString();
    }

    private static Action<T, object?> CreateSetter(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var valueParam = Expression.Parameter(typeof(object), "value");

        // Convert the object value to the property type
        var convertedValue = Expression.Convert(valueParam, property.PropertyType);

        // Create property assignment
        var propertyAccess = Expression.Property(instanceParam, property);
        var assign = Expression.Assign(propertyAccess, convertedValue);

        // Handle nullable types - check for null before assignment
        var propertyType = property.PropertyType;
        var isNullable = !propertyType.IsValueType ||
                         Nullable.GetUnderlyingType(propertyType) != null;

        Expression body;
        if (isNullable)
        {
            // If nullable, assign directly (null is valid)
            body = assign;
        }
        else
        {
            // For non-nullable value types, only assign if value is not null
            var nullCheck = Expression.NotEqual(valueParam, Expression.Constant(null));
            body = Expression.IfThen(nullCheck, assign);
        }

        var lambda = Expression.Lambda<Action<T, object?>>(body, instanceParam, valueParam);
        return lambda.Compile();
    }

    private static object? ConvertValue(object? value, Type targetType)
    {
        if (value is null)
        {
            return null;
        }

        var valueType = value.GetType();

        // Direct type match
        if (targetType.IsAssignableFrom(valueType))
        {
            return value;
        }

        // Handle Nullable<T>
        var underlyingType = Nullable.GetUnderlyingType(targetType);
        if (underlyingType != null)
        {
            return ConvertValue(value, underlyingType);
        }

        // Handle enum conversion
        if (targetType.IsEnum)
        {
            if (value is string stringValue)
                return Enum.Parse(targetType, stringValue);
            return Enum.ToObject(targetType, value);
        }

        // General conversion
        try
        {
            return Convert.ChangeType(value, targetType);
        }
        catch (InvalidCastException)
        {
            // Return null for incompatible types
            return null;
        }
    }
}

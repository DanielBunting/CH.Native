using System.Linq.Expressions;
using System.Reflection;
using CH.Native.Mapping;

namespace CH.Native.Results;

/// <summary>
/// Maps rows from a ClickHouseDataReader to instances of type T using reflection.
/// </summary>
/// <remarks>
/// <para>
/// Two construction strategies are supported, selected per-T at construction time:
/// </para>
/// <list type="number">
/// <item><description><b>Parameterless ctor + setter</b> — when T has a public parameterless
///     constructor (the classic POCO with init-only or settable properties).</description></item>
/// <item><description><b>Args ctor</b> — when T has a single public constructor that takes
///     all properties as arguments (positional records, anonymous types,
///     immutable read-only POCOs). Column values are buffered and the ctor is
///     invoked via <see cref="ConstructorInfo.Invoke(object[])"/>.</description></item>
/// </list>
/// <para>
/// The <c>where T : class</c> constraint replaces the historical <c>where T : new()</c>
/// — that constraint excluded records and anon types at compile time even
/// though the args-ctor path can materialize them at runtime.
/// </para>
/// </remarks>
/// <typeparam name="T">The target type to map to.</typeparam>
internal sealed class TypeMapper<T>
{
    private readonly bool _useArgsCtor;
    private readonly PropertyMap[] _propertyMaps;
    private readonly ConstructorInfo? _argsCtor;
    private readonly int[] _argsCtorOrdinals;
    private readonly Type[] _argsCtorParamTypes;

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
        // Strategy selection: prefer the parameterless ctor when present, else
        // fall back to the args-ctor path. Records and anon types only have
        // the args-ctor; setter-friendly POCOs have both — we prefer setters
        // because they're cheaper at map time (no per-row Activator.CreateInstance
        // followed by reflection.Invoke).
        var t = typeof(T);
        var parameterlessCtor = t.GetConstructor(
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types: Type.EmptyTypes,
            modifiers: null);

        // Value types (int, decimal, DateTime, etc.) always have a default
        // (zero) ctor available even without an explicit parameterless ctor.
        // The reflection lookup above misses that case; treat value types
        // as parameterless-ctor-eligible and let Activator.CreateInstance
        // produce default(T) before the property setters run. (For pure
        // scalars the property loop is a no-op and the result is just the
        // first column's value via Convert.ChangeType — that's the
        // QueryAsync<int>("SELECT 1") path.)
        if (parameterlessCtor is not null || t.IsValueType)
        {
            _useArgsCtor = false;
            _propertyMaps = BuildPropertyMaps(reader);
            _argsCtor = null;
            _argsCtorOrdinals = Array.Empty<int>();
            _argsCtorParamTypes = Array.Empty<Type>();
        }
        else
        {
            // Pick the public ctor with the most parameters (records / anon
            // types have exactly one; immutable POCOs may have several
            // overloads — prefer the maximal one for column coverage).
            var ctors = t.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            if (ctors.Length == 0)
                throw new InvalidOperationException(
                    $"Type '{t.FullName}' has no public constructor; cannot map rows to it.");

            _argsCtor = ctors.OrderByDescending(c => c.GetParameters().Length).First();
            var parameters = _argsCtor.GetParameters();
            _argsCtorOrdinals = new int[parameters.Length];
            _argsCtorParamTypes = new Type[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                _argsCtorParamTypes[i] = parameters[i].ParameterType;
                // Column name resolution order: [property:ClickHouseColumn(Name=...)]
                // on the matching property (records propagate property attributes
                // through the [property:] target), else [ClickHouseColumn(Name=...)]
                // on the parameter, else the parameter name, else snake_case
                // of the parameter name.
                var columnName = ResolveColumnNameForCtorParam(t, parameters[i]);
                _argsCtorOrdinals[i] = TryGetOrdinal(reader, columnName);
                if (_argsCtorOrdinals[i] < 0)
                    throw new InvalidOperationException(
                        $"Type '{t.FullName}' constructor parameter '{parameters[i].Name}' " +
                        $"does not match any column in the result set (looked for '{columnName}').");
            }

            _useArgsCtor = true;
            _propertyMaps = Array.Empty<PropertyMap>();
        }
    }

    /// <summary>
    /// Maps the current row of the reader to a new instance of T.
    /// </summary>
    /// <param name="reader">The data reader positioned on a row.</param>
    /// <returns>A new instance of T with mapped values.</returns>
    public T Map(ClickHouseDataReader reader)
    {
        if (_useArgsCtor)
            return MapViaArgsCtor(reader);

        var instance = Activator.CreateInstance<T>();
        foreach (ref readonly var map in _propertyMaps.AsSpan())
        {
            var value = reader.GetValue(map.Ordinal);
            var convertedValue = ConvertValue(value, map.PropertyType);
            map.Setter(instance, convertedValue);
        }
        return instance;
    }

    private T MapViaArgsCtor(ClickHouseDataReader reader)
    {
        var args = new object?[_argsCtorOrdinals.Length];
        for (int i = 0; i < args.Length; i++)
        {
            var raw = reader.GetValue(_argsCtorOrdinals[i]);
            args[i] = ConvertValue(raw, _argsCtorParamTypes[i]);
        }
        return (T)_argsCtor!.Invoke(args);
    }

    private static string ResolveColumnNameForCtorParam(Type t, ParameterInfo param)
    {
        // Records: [property: ClickHouseColumn(Name = "x")] applied to a
        // positional ctor param ends up on the generated property, not the
        // parameter. Look for a property whose name matches the parameter
        // name (records mirror them) and read the attribute off the property.
        var matchingProp = t.GetProperty(param.Name ?? "",
            BindingFlags.Public | BindingFlags.Instance);
        if (matchingProp is not null)
        {
            var propAttr = matchingProp.GetCustomAttribute<ClickHouseColumnAttribute>();
            // Ignore=true on a property that mirrors a required ctor arg is
            // a malformed POCO — silently substituting default(T) for the
            // arg would corrupt user data. Surface a typed diagnostic so
            // the misuse is loud at construction time.
            if (propAttr?.Ignore == true)
                throw new InvalidOperationException(
                    $"Type '{t.FullName}' has [ClickHouseColumn(Ignore = true)] on " +
                    $"property '{matchingProp.Name}' that backs constructor parameter " +
                    $"'{param.Name}'. Ignored properties cannot also be required " +
                    "constructor arguments — drop Ignore or remove the property from the ctor.");
            if (!string.IsNullOrEmpty(propAttr?.Name))
                return propAttr.Name;
        }

        // Custom POCOs may attach the attribute directly to the ctor param.
        var paramAttr = param.GetCustomAttribute<ClickHouseColumnAttribute>();
        if (paramAttr?.Ignore == true)
            throw new InvalidOperationException(
                $"Type '{t.FullName}' has [ClickHouseColumn(Ignore = true)] on " +
                $"constructor parameter '{param.Name}'. Ignored parameters cannot " +
                "be required ctor arguments — drop Ignore or remove the parameter.");
        if (!string.IsNullOrEmpty(paramAttr?.Name))
            return paramAttr.Name;

        return param.Name ?? string.Empty;
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

            // Honor [ClickHouseColumn(Name = "...")] on properties.
            var attr = prop.GetCustomAttribute<ClickHouseColumnAttribute>();
            // [ClickHouseColumn(Ignore = true)] excludes the property from
            // both write and read mapping — see attribute XML doc. Skip
            // before the column-lookup so the property is invisible to the
            // mapper regardless of whether a matching column exists.
            if (attr?.Ignore == true)
                continue;

            var lookupName = !string.IsNullOrEmpty(attr?.Name) ? attr.Name : prop.Name;

            var ordinal = TryGetOrdinal(reader, lookupName);
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

        // Jagged → rectangular: column readers always materialize Array(Array(T))
        // as jagged. If the caller asks for T[,] (or higher rank), validate uniform
        // shape and copy into the rect form.
        if (targetType.IsArray && targetType.GetArrayRank() > 1 && value is Array jaggedArray)
        {
            return Data.Conversion.JaggedToRectangularConverter.ToRectangular(jaggedArray, targetType);
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

using System.Collections;

namespace CH.Native.Commands;

/// <summary>
/// A collection of ClickHouseParameter objects.
/// </summary>
public sealed class ClickHouseParameterCollection : IList<ClickHouseParameter>
{
    private readonly List<ClickHouseParameter> _parameters = new();
    private readonly Dictionary<string, int> _lookup = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the number of parameters in the collection.
    /// </summary>
    public int Count => _parameters.Count;

    /// <summary>
    /// Gets a value indicating whether the collection is read-only.
    /// </summary>
    public bool IsReadOnly => false;

    /// <summary>
    /// Gets or sets the parameter at the specified index.
    /// </summary>
    public ClickHouseParameter this[int index]
    {
        get => _parameters[index];
        set
        {
            var oldParam = _parameters[index];
            if (!string.IsNullOrEmpty(oldParam.ParameterName))
                _lookup.Remove(oldParam.ParameterName);

            _parameters[index] = value;
            if (!string.IsNullOrEmpty(value.ParameterName))
                _lookup[value.ParameterName] = index;
        }
    }

    /// <summary>
    /// Gets the parameter with the specified name.
    /// </summary>
    /// <param name="name">The parameter name (case-insensitive).</param>
    /// <returns>The parameter with the specified name.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the parameter is not found.</exception>
    public ClickHouseParameter this[string name]
    {
        get
        {
            if (_lookup.TryGetValue(name.TrimStart('@'), out var index))
                return _parameters[index];
            throw new KeyNotFoundException($"Parameter '{name}' not found.");
        }
    }

    /// <summary>
    /// Adds a new parameter with the specified name and value.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>The newly created parameter.</returns>
    public ClickHouseParameter Add(string name, object? value)
    {
        var param = new ClickHouseParameter(name, value);
        Add(param);
        return param;
    }

    /// <summary>
    /// Adds a new parameter with the specified name, value, and explicit type.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <param name="clickHouseType">The explicit ClickHouse type name.</param>
    /// <returns>The newly created parameter.</returns>
    public ClickHouseParameter Add(string name, object? value, string clickHouseType)
    {
        var param = new ClickHouseParameter(name, value, clickHouseType);
        Add(param);
        return param;
    }

    /// <summary>
    /// Adds a parameter to the collection.
    /// </summary>
    /// <param name="item">The parameter to add.</param>
    public void Add(ClickHouseParameter item)
    {
        ArgumentNullException.ThrowIfNull(item);

        if (!string.IsNullOrEmpty(item.ParameterName))
        {
            if (_lookup.ContainsKey(item.ParameterName))
                throw new ArgumentException($"Parameter '{item.ParameterName}' already exists.", nameof(item));
            _lookup[item.ParameterName] = _parameters.Count;
        }
        _parameters.Add(item);
    }

    /// <summary>
    /// Determines whether the collection contains a parameter with the specified name.
    /// </summary>
    /// <param name="parameterName">The parameter name (case-insensitive).</param>
    /// <returns>true if a parameter with the name exists; otherwise, false.</returns>
    public bool Contains(string parameterName)
    {
        return _lookup.ContainsKey(parameterName.TrimStart('@'));
    }

    /// <summary>
    /// Determines whether the collection contains the specified parameter.
    /// </summary>
    public bool Contains(ClickHouseParameter item)
    {
        return _parameters.Contains(item);
    }

    /// <summary>
    /// Gets the index of the parameter with the specified name.
    /// </summary>
    /// <param name="parameterName">The parameter name (case-insensitive).</param>
    /// <returns>The index of the parameter, or -1 if not found.</returns>
    public int IndexOf(string parameterName)
    {
        return _lookup.TryGetValue(parameterName.TrimStart('@'), out var index) ? index : -1;
    }

    /// <summary>
    /// Gets the index of the specified parameter.
    /// </summary>
    public int IndexOf(ClickHouseParameter item)
    {
        return _parameters.IndexOf(item);
    }

    /// <summary>
    /// Inserts a parameter at the specified index.
    /// </summary>
    public void Insert(int index, ClickHouseParameter item)
    {
        ArgumentNullException.ThrowIfNull(item);

        if (!string.IsNullOrEmpty(item.ParameterName))
        {
            if (_lookup.ContainsKey(item.ParameterName))
                throw new ArgumentException($"Parameter '{item.ParameterName}' already exists.", nameof(item));
        }

        _parameters.Insert(index, item);
        RebuildLookup();
    }

    /// <summary>
    /// Removes the parameter at the specified index.
    /// </summary>
    public void RemoveAt(int index)
    {
        var param = _parameters[index];
        _parameters.RemoveAt(index);
        if (!string.IsNullOrEmpty(param.ParameterName))
            RebuildLookup();
    }

    /// <summary>
    /// Removes the parameter with the specified name.
    /// </summary>
    /// <param name="parameterName">The parameter name.</param>
    /// <returns>true if the parameter was removed; otherwise, false.</returns>
    public bool Remove(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
        {
            RemoveAt(index);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Removes the specified parameter from the collection.
    /// </summary>
    public bool Remove(ClickHouseParameter item)
    {
        var index = _parameters.IndexOf(item);
        if (index >= 0)
        {
            RemoveAt(index);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Removes all parameters from the collection.
    /// </summary>
    public void Clear()
    {
        _parameters.Clear();
        _lookup.Clear();
    }

    /// <summary>
    /// Copies the parameters to an array.
    /// </summary>
    public void CopyTo(ClickHouseParameter[] array, int arrayIndex)
    {
        _parameters.CopyTo(array, arrayIndex);
    }

    /// <summary>
    /// Returns an enumerator that iterates through the collection.
    /// </summary>
    public IEnumerator<ClickHouseParameter> GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private void RebuildLookup()
    {
        _lookup.Clear();
        for (int i = 0; i < _parameters.Count; i++)
        {
            var param = _parameters[i];
            if (!string.IsNullOrEmpty(param.ParameterName))
                _lookup[param.ParameterName] = i;
        }
    }
}

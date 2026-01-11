using System.Collections;
using System.Data.Common;

namespace CH.Native.Ado;

/// <summary>
/// A ClickHouse-specific implementation of <see cref="DbParameterCollection"/>.
/// </summary>
public sealed class ClickHouseDbParameterCollection : DbParameterCollection
{
    private readonly List<ClickHouseDbParameter> _parameters = new();
    private readonly Dictionary<string, int> _lookup = new(StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public override int Count => _parameters.Count;

    /// <inheritdoc />
    public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

    /// <inheritdoc />
    public override int Add(object value)
    {
        var param = (ClickHouseDbParameter)value;
        ValidateNoDuplicate(param.ParameterName);
        _lookup[NormalizeName(param.ParameterName)] = _parameters.Count;
        _parameters.Add(param);
        return _parameters.Count - 1;
    }

    /// <summary>
    /// Adds a parameter with the specified name and value.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>The created parameter.</returns>
    public ClickHouseDbParameter Add(string name, object? value)
    {
        var param = new ClickHouseDbParameter { ParameterName = name, Value = value };
        Add(param);
        return param;
    }

    /// <summary>
    /// Adds a parameter with the specified name, value, and explicit ClickHouse type.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    /// <param name="clickHouseType">The explicit ClickHouse type (e.g., "Int32", "DateTime64(3)").</param>
    /// <returns>The created parameter.</returns>
    public ClickHouseDbParameter Add(string name, object? value, string clickHouseType)
    {
        var param = new ClickHouseDbParameter
        {
            ParameterName = name,
            Value = value,
            ClickHouseType = clickHouseType
        };
        Add(param);
        return param;
    }

    /// <inheritdoc />
    public override void Clear()
    {
        _parameters.Clear();
        _lookup.Clear();
    }

    /// <inheritdoc />
    public override bool Contains(object value)
    {
        return _parameters.Contains((ClickHouseDbParameter)value);
    }

    /// <inheritdoc />
    public override bool Contains(string value)
    {
        return _lookup.ContainsKey(NormalizeName(value));
    }

    /// <inheritdoc />
    public override void CopyTo(Array array, int index)
    {
        ((ICollection)_parameters).CopyTo(array, index);
    }

    /// <inheritdoc />
    public override IEnumerator GetEnumerator()
    {
        return _parameters.GetEnumerator();
    }

    /// <inheritdoc />
    public override int IndexOf(object value)
    {
        return _parameters.IndexOf((ClickHouseDbParameter)value);
    }

    /// <inheritdoc />
    public override int IndexOf(string parameterName)
    {
        return _lookup.TryGetValue(NormalizeName(parameterName), out var index) ? index : -1;
    }

    /// <inheritdoc />
    public override void Insert(int index, object value)
    {
        var param = (ClickHouseDbParameter)value;
        ValidateNoDuplicate(param.ParameterName);
        _parameters.Insert(index, param);
        RebuildLookup();
    }

    /// <inheritdoc />
    public override void Remove(object value)
    {
        var param = (ClickHouseDbParameter)value;
        if (_parameters.Remove(param))
            RebuildLookup();
    }

    /// <inheritdoc />
    public override void RemoveAt(int index)
    {
        _parameters.RemoveAt(index);
        RebuildLookup();
    }

    /// <inheritdoc />
    public override void RemoveAt(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
            RemoveAt(index);
    }

    /// <inheritdoc />
    protected override DbParameter GetParameter(int index)
    {
        return _parameters[index];
    }

    /// <inheritdoc />
    protected override DbParameter GetParameter(string parameterName)
    {
        var index = IndexOf(parameterName);
        if (index < 0)
            throw new ArgumentException($"Parameter '{parameterName}' not found.", nameof(parameterName));
        return _parameters[index];
    }

    /// <inheritdoc />
    protected override void SetParameter(int index, DbParameter value)
    {
        var oldParam = _parameters[index];
        var newParam = (ClickHouseDbParameter)value;

        // If the name changed and it's a different name, validate no duplicate
        if (!string.Equals(NormalizeName(oldParam.ParameterName), NormalizeName(newParam.ParameterName), StringComparison.OrdinalIgnoreCase))
        {
            ValidateNoDuplicate(newParam.ParameterName);
        }

        _parameters[index] = newParam;
        RebuildLookup();
    }

    /// <inheritdoc />
    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var index = IndexOf(parameterName);
        if (index >= 0)
            SetParameter(index, value);
        else
            Add(value);
    }

    /// <inheritdoc />
    public override void AddRange(Array values)
    {
        foreach (var value in values)
            Add(value!);
    }

    private static string NormalizeName(string name)
    {
        // Strip leading @ if present (ADO.NET convention)
        return name.StartsWith('@') ? name.Substring(1) : name;
    }

    private void ValidateNoDuplicate(string name)
    {
        if (_lookup.ContainsKey(NormalizeName(name)))
            throw new ArgumentException($"A parameter with name '{name}' already exists.", nameof(name));
    }

    private void RebuildLookup()
    {
        _lookup.Clear();
        for (int i = 0; i < _parameters.Count; i++)
        {
            _lookup[NormalizeName(_parameters[i].ParameterName)] = i;
        }
    }
}

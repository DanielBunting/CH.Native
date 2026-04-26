using CH.Native.Data.Types;
using CH.Native.Parameters;

namespace CH.Native.Commands;

/// <summary>
/// Represents a parameter for a ClickHouse query.
/// </summary>
public sealed class ClickHouseParameter
{
    private string _parameterName = string.Empty;

    /// <summary>
    /// Gets or sets the parameter name (without @ prefix).
    /// </summary>
    public string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value?.TrimStart('@') ?? string.Empty;
    }

    /// <summary>
    /// Gets or sets the parameter value.
    /// </summary>
    public object? Value { get; set; }

    /// <summary>
    /// Gets or sets the explicit ClickHouse type name.
    /// If null, the type is inferred from Value.
    /// </summary>
    /// <remarks>
    /// The setter validates the type string through <see cref="ClickHouseTypeParser.Parse"/>.
    /// This is the security gate that prevents SQL injection through the {name:Type}
    /// placeholder in <see cref="SqlParameterRewriter.Rewrite"/>: any string containing
    /// SQL syntax (semicolons, comments, mismatched braces, etc.) is rejected before
    /// it can reach the wire. Surrounding whitespace is trimmed.
    /// </remarks>
    /// <exception cref="ArgumentException">Thrown when the type name is malformed.</exception>
    public string? ClickHouseType
    {
        get => _clickHouseType;
        set
        {
            if (value is null)
            {
                _clickHouseType = null;
                return;
            }

            var trimmed = value.Trim();
            try
            {
                ClickHouseTypeParser.Parse(trimmed);
            }
            catch (FormatException ex)
            {
                throw new ArgumentException(
                    $"Invalid ClickHouse type name '{value}': {ex.Message}",
                    nameof(value),
                    ex);
            }

            _clickHouseType = trimmed;
        }
    }
    private string? _clickHouseType;

    /// <summary>
    /// Gets the effective ClickHouse type name (explicit or inferred).
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when type cannot be inferred from null value.</exception>
    /// <exception cref="NotSupportedException">Thrown when type cannot be inferred.</exception>
    public string ResolvedTypeName => ClickHouseType ?? ClickHouseTypeMapper.InferType(Value);

    /// <summary>
    /// Creates a new empty parameter.
    /// </summary>
    public ClickHouseParameter()
    {
    }

    /// <summary>
    /// Creates a new parameter with the specified name and value.
    /// </summary>
    /// <param name="name">The parameter name (with or without @ prefix).</param>
    /// <param name="value">The parameter value.</param>
    public ClickHouseParameter(string name, object? value)
    {
        ParameterName = name;
        Value = value;
    }

    /// <summary>
    /// Creates a new parameter with the specified name, value, and explicit type.
    /// </summary>
    /// <param name="name">The parameter name (with or without @ prefix).</param>
    /// <param name="value">The parameter value.</param>
    /// <param name="clickHouseType">The explicit ClickHouse type name.</param>
    public ClickHouseParameter(string name, object? value, string clickHouseType)
        : this(name, value)
    {
        ClickHouseType = clickHouseType;
    }
}

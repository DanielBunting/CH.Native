using System.Data;
using System.Data.Common;
using CH.Native.Data.Types;

namespace CH.Native.Ado;

/// <summary>
/// A ClickHouse-specific implementation of <see cref="DbParameter"/>.
/// </summary>
public sealed class ClickHouseDbParameter : DbParameter
{
    /// <inheritdoc />
    public override DbType DbType { get; set; }

    /// <inheritdoc />
    public override ParameterDirection Direction
    {
        get => ParameterDirection.Input;
        set
        {
            if (value != ParameterDirection.Input)
                throw new NotSupportedException("ClickHouse only supports input parameters.");
        }
    }

    /// <inheritdoc />
    public override bool IsNullable { get; set; }

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string ParameterName
    {
        get => _parameterName;
        set => _parameterName = value ?? "";
    }
    private string _parameterName = "";

    /// <inheritdoc />
    public override int Size { get; set; }

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.AllowNull]
    public override string SourceColumn
    {
        get => _sourceColumn;
        set => _sourceColumn = value ?? "";
    }
    private string _sourceColumn = "";

    /// <inheritdoc />
    public override bool SourceColumnNullMapping { get; set; }

    /// <inheritdoc />
    public override object? Value { get; set; }

    /// <summary>
    /// Gets or sets the explicit ClickHouse type (e.g., "Int32", "String", "DateTime64(3)").
    /// If not set, the type is inferred from <see cref="Value"/> via ClickHouseTypeMapper.
    /// </summary>
    /// <remarks>
    /// The setter validates the type string through <see cref="ClickHouseTypeParser.Parse"/>
    /// to prevent SQL injection through the wire {name:Type} placeholder. Surrounding
    /// whitespace is trimmed.
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

    /// <inheritdoc />
    public override void ResetDbType()
    {
        DbType = DbType.Object;
    }
}

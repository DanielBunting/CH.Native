using System.Data;
using System.Data.Common;

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
    public string? ClickHouseType { get; set; }

    /// <inheritdoc />
    public override void ResetDbType()
    {
        DbType = DbType.Object;
    }
}

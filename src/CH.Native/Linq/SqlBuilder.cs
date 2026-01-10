using System.Globalization;
using System.Text;

namespace CH.Native.Linq;

/// <summary>
/// Builds ClickHouse SQL queries from LINQ expression tree traversal.
/// </summary>
internal sealed class SqlBuilder
{
    private readonly List<string> _selectColumns = new();
    private readonly List<string> _whereClauses = new();
    private readonly List<(string Column, bool Descending)> _orderByColumns = new();
    private readonly List<string> _groupByColumns = new();
    private string? _havingClause;

    private string? _tableName;
    private bool _useFinal;
    private double? _sampleRatio;
    private int? _limit;
    private int? _offset;
    private bool _distinct;

    // Aggregate tracking
    private string? _aggregateFunction;
    private string? _aggregateColumn;

    /// <summary>
    /// Sets the table name.
    /// </summary>
    public SqlBuilder Table(string tableName)
    {
        _tableName = tableName;
        return this;
    }

    /// <summary>
    /// Adds a column to the SELECT clause.
    /// </summary>
    public SqlBuilder Select(string column)
    {
        _selectColumns.Add(column);
        return this;
    }

    /// <summary>
    /// Adds a column with alias to the SELECT clause.
    /// </summary>
    public SqlBuilder Select(string expression, string alias)
    {
        _selectColumns.Add($"{expression} AS {QuoteIdentifier(alias)}");
        return this;
    }

    /// <summary>
    /// Sets the SELECT to use an aggregate function.
    /// </summary>
    public SqlBuilder SetAggregate(string function, string? column = null)
    {
        _aggregateFunction = function;
        _aggregateColumn = column;
        return this;
    }

    /// <summary>
    /// Appends to the WHERE clause with AND logic.
    /// </summary>
    public SqlBuilder Where(string condition)
    {
        _whereClauses.Add(condition);
        return this;
    }

    /// <summary>
    /// Adds an ORDER BY column.
    /// </summary>
    public SqlBuilder OrderBy(string column, bool descending = false)
    {
        _orderByColumns.Add((column, descending));
        return this;
    }

    /// <summary>
    /// Clears existing ORDER BY and adds a new column.
    /// </summary>
    public SqlBuilder ClearAndOrderBy(string column, bool descending = false)
    {
        _orderByColumns.Clear();
        _orderByColumns.Add((column, descending));
        return this;
    }

    /// <summary>
    /// Adds a GROUP BY column.
    /// </summary>
    public SqlBuilder GroupBy(string column)
    {
        _groupByColumns.Add(column);
        return this;
    }

    /// <summary>
    /// Sets the HAVING clause.
    /// </summary>
    public SqlBuilder Having(string condition)
    {
        if (_havingClause is not null)
        {
            _havingClause = $"({_havingClause}) AND ({condition})";
        }
        else
        {
            _havingClause = condition;
        }
        return this;
    }

    /// <summary>
    /// Sets LIMIT (Take).
    /// </summary>
    public SqlBuilder Limit(int count)
    {
        _limit = count;
        return this;
    }

    /// <summary>
    /// Sets OFFSET (Skip).
    /// </summary>
    public SqlBuilder Offset(int count)
    {
        _offset = count;
        return this;
    }

    /// <summary>
    /// Enables the FINAL clause for ReplacingMergeTree/CollapsingMergeTree.
    /// </summary>
    public SqlBuilder Final()
    {
        _useFinal = true;
        return this;
    }

    /// <summary>
    /// Sets SAMPLE ratio (0.0 to 1.0).
    /// </summary>
    public SqlBuilder Sample(double ratio)
    {
        if (ratio <= 0 || ratio > 1)
            throw new ArgumentOutOfRangeException(nameof(ratio), "Sample ratio must be between 0 and 1");
        _sampleRatio = ratio;
        return this;
    }

    /// <summary>
    /// Enables DISTINCT.
    /// </summary>
    public SqlBuilder Distinct()
    {
        _distinct = true;
        return this;
    }

    /// <summary>
    /// Gets whether any SELECT columns have been specified.
    /// </summary>
    public bool HasSelectColumns => _selectColumns.Count > 0;

    /// <summary>
    /// Gets whether an aggregate function has been set.
    /// </summary>
    public bool HasAggregate => _aggregateFunction is not null;

    /// <summary>
    /// Generates the final SQL string.
    /// </summary>
    public string Build()
    {
        var sb = new StringBuilder();

        // SELECT clause
        sb.Append("SELECT ");
        if (_distinct)
            sb.Append("DISTINCT ");

        if (_aggregateFunction is not null)
        {
            // Aggregate query
            if (_aggregateColumn is not null)
            {
                sb.Append($"{_aggregateFunction}({_aggregateColumn})");
            }
            else
            {
                sb.Append($"{_aggregateFunction}()");
            }
        }
        else if (_selectColumns.Count > 0)
        {
            sb.Append(string.Join(", ", _selectColumns));
        }
        else
        {
            sb.Append('*');
        }

        // FROM clause
        sb.Append(" FROM ");
        sb.Append(_tableName);

        // FINAL clause (ClickHouse-specific)
        if (_useFinal)
            sb.Append(" FINAL");

        // SAMPLE clause (ClickHouse-specific)
        if (_sampleRatio.HasValue)
            sb.Append($" SAMPLE {_sampleRatio.Value.ToString(CultureInfo.InvariantCulture)}");

        // WHERE clause
        if (_whereClauses.Count > 0)
        {
            sb.Append(" WHERE ");
            if (_whereClauses.Count == 1)
            {
                sb.Append(_whereClauses[0]);
            }
            else
            {
                sb.Append(string.Join(" AND ", _whereClauses.Select(w => $"({w})")));
            }
        }

        // GROUP BY clause
        if (_groupByColumns.Count > 0)
        {
            sb.Append(" GROUP BY ");
            sb.Append(string.Join(", ", _groupByColumns));
        }

        // HAVING clause
        if (_havingClause is not null)
        {
            sb.Append(" HAVING ");
            sb.Append(_havingClause);
        }

        // ORDER BY clause (skip if aggregate without group by)
        if (_orderByColumns.Count > 0 && _aggregateFunction is null)
        {
            sb.Append(" ORDER BY ");
            sb.Append(string.Join(", ", _orderByColumns.Select(o =>
                o.Descending ? $"{o.Column} DESC" : o.Column)));
        }

        // LIMIT/OFFSET
        if (_limit.HasValue)
            sb.Append($" LIMIT {_limit.Value}");
        if (_offset.HasValue)
            sb.Append($" OFFSET {_offset.Value}");

        return sb.ToString();
    }

    /// <summary>
    /// Quotes an identifier with backticks for ClickHouse.
    /// </summary>
    public static string QuoteIdentifier(string identifier)
    {
        // Escape backticks within the identifier
        return $"`{identifier.Replace("`", "``")}`";
    }
}

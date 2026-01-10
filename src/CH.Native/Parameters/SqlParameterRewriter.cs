using System.Text.RegularExpressions;
using CH.Native.Commands;

namespace CH.Native.Parameters;

/// <summary>
/// Rewrites SQL with @param syntax to ClickHouse {param:Type} syntax
/// and builds parameter settings for the query.
/// </summary>
public static partial class SqlParameterRewriter
{
    // Matches @parameterName patterns
    // - Must start with @ followed by letter or underscore
    // - Can contain letters, digits, underscores
    // - Negative lookbehind (?<!@) prevents matching @@variable
    [GeneratedRegex(@"(?<!@)@([a-zA-Z_][a-zA-Z0-9_]*)", RegexOptions.Compiled)]
    private static partial Regex ParameterPattern();

    /// <summary>
    /// Rewrites SQL from @param syntax to {param:Type} syntax.
    /// </summary>
    /// <param name="sql">The original SQL with @param placeholders.</param>
    /// <param name="parameters">The parameter collection.</param>
    /// <returns>The rewritten SQL with ClickHouse parameter syntax.</returns>
    /// <exception cref="ArgumentException">Thrown when a referenced parameter is not provided.</exception>
    public static string Rewrite(string sql, ClickHouseParameterCollection parameters)
    {
        if (parameters.Count == 0)
            return sql;

        return ParameterPattern().Replace(sql, match =>
        {
            var name = match.Groups[1].Value;

            if (!parameters.Contains(name))
            {
                throw new ArgumentException(
                    $"Parameter '@{name}' is referenced in SQL but not provided in the parameter collection.",
                    nameof(parameters));
            }

            var param = parameters[name];
            var typeName = param.ResolvedTypeName;

            // Rewrite to ClickHouse format: {name:Type}
            return $"{{{param.ParameterName}:{typeName}}}";
        });
    }

    /// <summary>
    /// Builds the parameters dictionary for query parameters.
    /// Each parameter name maps to its serialized value.
    /// </summary>
    /// <param name="parameters">The parameter collection.</param>
    /// <returns>Dictionary of parameter names to serialized values.</returns>
    public static Dictionary<string, string> BuildParameterSettings(ClickHouseParameterCollection parameters)
    {
        var result = new Dictionary<string, string>(parameters.Count);

        foreach (var param in parameters)
        {
            // Parameter name is used directly (no prefix needed in native protocol)
            var parameterValue = ParameterSerializer.Serialize(param.Value, param.ResolvedTypeName);
            result[param.ParameterName] = parameterValue;
        }

        return result;
    }

    /// <summary>
    /// Processes SQL and parameters into a rewritten SQL string and settings dictionary.
    /// </summary>
    /// <param name="sql">The original SQL with @param placeholders.</param>
    /// <param name="parameters">The parameter collection.</param>
    /// <returns>A tuple of (rewritten SQL, settings dictionary).</returns>
    public static (string Sql, Dictionary<string, string> Settings) Process(
        string sql,
        ClickHouseParameterCollection parameters)
    {
        var rewrittenSql = Rewrite(sql, parameters);
        var settings = BuildParameterSettings(parameters);
        return (rewrittenSql, settings);
    }
}

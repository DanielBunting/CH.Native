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

    // ClickHouse 26.x's parser misinterprets {limit:Type} / {offset:Type} as
    // the start of a LIMIT/OFFSET clause and rejects the rewritten SQL with
    // CANNOT_PARSE_QUOTED_STRING ("expected opening quote ''', got '1'").
    // Other SQL keywords (select, from, where, group, order, …) work fine as
    // placeholder names; only the two tail-clause keywords that take a numeric
    // argument trip the parser. Reject these names early so callers get a
    // diagnostic pointing at the workaround instead of a cryptic server error.
    // See: https://github.com/ClickHouse/ClickHouse/issues -- reserved tail-clause keywords.
    private static readonly HashSet<string> s_unsafeParameterNames =
        new(StringComparer.OrdinalIgnoreCase) { "limit", "offset" };

    /// <summary>
    /// Rewrites SQL from @param syntax to {param:Type} syntax.
    /// </summary>
    /// <param name="sql">The original SQL with @param placeholders.</param>
    /// <param name="parameters">The parameter collection.</param>
    /// <returns>The rewritten SQL with ClickHouse parameter syntax.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when a referenced parameter is not provided, or when a parameter
    /// is named <c>limit</c>/<c>offset</c> — ClickHouse's parser cannot accept
    /// those as placeholder names.
    /// </exception>
    public static string Rewrite(string sql, ClickHouseParameterCollection parameters)
    {
        if (parameters.Count == 0)
            return sql;

        foreach (var p in parameters)
        {
            if (s_unsafeParameterNames.Contains(p.ParameterName))
            {
                throw new ArgumentException(
                    $"Parameter name '{p.ParameterName}' collides with a ClickHouse tail-clause keyword. " +
                    $"The server parser misinterprets `{{{p.ParameterName}:Type}}` as the start of a " +
                    $"LIMIT/OFFSET clause and rejects the query with CANNOT_PARSE_QUOTED_STRING. " +
                    $"Rename the parameter (e.g. '{p.ParameterName}_value' or 'max_{p.ParameterName}').",
                    nameof(parameters));
            }
        }

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

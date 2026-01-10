using System.Text;
using CH.Native.Mapping;

namespace CH.Native.Linq;

/// <summary>
/// Resolves ClickHouse table and column names for entity types.
/// Uses generated mappers when available, falls back to snake_case convention.
/// </summary>
internal static class TableNameResolver
{
    /// <summary>
    /// Resolves the table name for type T.
    /// Priority:
    /// 1. Generated mapper's TableName (from [ClickHouseTable] attribute)
    /// 2. Snake_case conversion of type name
    /// </summary>
    public static string Resolve<T>()
    {
        if (GeneratedMapperHelper.TryGetTableName<T>(out var tableName) && tableName is not null)
        {
            return tableName;
        }

        return ToSnakeCase(typeof(T).Name);
    }

    /// <summary>
    /// Gets column names from generated mapper if available.
    /// </summary>
    public static string[]? GetColumnNames<T>()
    {
        if (GeneratedMapperHelper.TryGetColumnNames<T>(out var columnNames))
        {
            return columnNames;
        }
        return null;
    }

    /// <summary>
    /// Resolves a property name to a column name.
    /// Uses generated mapper column names if available, otherwise converts to snake_case.
    /// </summary>
    public static string ResolveColumnName<T>(string propertyName)
    {
        // For now, use snake_case conversion
        // In the future, we could add property-to-column mapping from generated mapper
        return ToSnakeCase(propertyName);
    }

    /// <summary>
    /// Converts PascalCase to snake_case.
    /// Examples:
    ///   "UserAccount" -> "user_account"
    ///   "XMLParser" -> "xml_parser"
    ///   "ID" -> "id"
    ///   "OrderID" -> "order_id"
    /// </summary>
    internal static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var sb = new StringBuilder();

        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];

            if (char.IsUpper(c))
            {
                if (i > 0)
                {
                    // Add underscore before uppercase if:
                    // - Previous char was lowercase, OR
                    // - Previous char was uppercase AND next char is lowercase (handles "XMLParser" -> "xml_parser")
                    bool prevIsLower = char.IsLower(name[i - 1]);
                    bool nextIsLower = i + 1 < name.Length && char.IsLower(name[i + 1]);

                    if (prevIsLower || (char.IsUpper(name[i - 1]) && nextIsLower))
                    {
                        sb.Append('_');
                    }
                }
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }

        return sb.ToString();
    }
}

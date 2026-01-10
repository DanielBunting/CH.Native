using System.Text;

namespace CH.Native.Linq;

/// <summary>
/// Resolves ClickHouse table and column names for entity types.
/// Uses snake_case convention for name conversion.
/// </summary>
internal static class TableNameResolver
{
    /// <summary>
    /// Resolves the table name for type T using snake_case conversion.
    /// </summary>
    public static string Resolve<T>()
    {
        return ToSnakeCase(typeof(T).Name);
    }

    /// <summary>
    /// Resolves a property name to a column name using snake_case conversion.
    /// </summary>
    public static string ResolveColumnName<T>(string propertyName)
    {
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

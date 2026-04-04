using System.Text.RegularExpressions;

namespace NaturalQuery.Validation;

/// <summary>
/// Validates generated SQL queries for safety and correctness.
/// </summary>
public static class SqlValidator
{
    private static readonly string[] DefaultForbiddenKeywords =
    {
        "DELETE ", "UPDATE ", " INSERT INTO", "DROP ", "ALTER ",
        "CREATE ", "TRUNCATE ", "GRANT ", "REVOKE "
    };

    /// <summary>
    /// Validates that a SQL query is safe to execute.
    /// </summary>
    /// <param name="sql">The SQL query to validate.</param>
    /// <param name="tenantIdColumn">If set, the query must contain a filter on this column.</param>
    /// <param name="tenantId">The tenant ID that must appear in the query (when tenantIdColumn is set).</param>
    /// <param name="additionalForbidden">Extra keywords to block.</param>
    /// <returns>Null if valid, or an error message if invalid.</returns>
    public static string? Validate(
        string sql,
        string? tenantIdColumn = null,
        string? tenantId = null,
        IEnumerable<string>? additionalForbidden = null)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return "SQL query cannot be empty.";

        var sqlUpper = sql.Trim().ToUpperInvariant();

        // Must start with SELECT or WITH (CTE)
        if (!sqlUpper.StartsWith("SELECT") && !sqlUpper.StartsWith("WITH"))
            return "Only SELECT queries are allowed.";

        // Remove string literals to avoid false positives
        // e.g., _eventname IN ('INSERT', 'BACKFILL') — INSERT here is a value, not a command
        var sqlNoStrings = Regex.Replace(sqlUpper, "'[^']*'", "''");

        // Check forbidden keywords
        foreach (var keyword in DefaultForbiddenKeywords)
        {
            if (sqlNoStrings.Contains(keyword))
                return $"Forbidden SQL keyword detected: {keyword.Trim()}";
        }

        if (additionalForbidden != null)
        {
            foreach (var keyword in additionalForbidden)
            {
                if (sqlNoStrings.Contains(keyword.ToUpperInvariant()))
                    return $"Forbidden SQL keyword detected: {keyword.Trim()}";
            }
        }

        // Multi-statement detection — reject queries with unquoted semicolons
        var sqlNoStringsForSemicolon = Regex.Replace(sql, "'[^']*'", "''");
        if (sqlNoStringsForSemicolon.Contains(';'))
        {
            // Allow trailing semicolon only
            var trimmed = sqlNoStringsForSemicolon.TrimEnd();
            if (trimmed.IndexOf(';') < trimmed.Length - 1)
                return "Multiple SQL statements are not allowed.";
        }

        // Tenant isolation guard
        if (!string.IsNullOrEmpty(tenantIdColumn) && !string.IsNullOrEmpty(tenantId))
        {
            if (!sql.Contains(tenantId, StringComparison.OrdinalIgnoreCase))
                return $"Query must contain a filter on tenant column '{tenantIdColumn}'.";
        }

        return null; // Valid
    }
}

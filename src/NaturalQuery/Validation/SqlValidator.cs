using System.Text.RegularExpressions;

namespace NaturalQuery.Validation;

/// <summary>
/// Validates generated SQL queries for safety and correctness.
/// Runs the SqlNormalizer pipeline (comment stripping, literal removal, whitespace
/// collapse) under both block-comment dialect interpretations, then applies a
/// word-boundary denylist — so obfuscated dangerous operations cannot slip through.
/// </summary>
public static class SqlValidator
{
    // Word-boundary denylist over normalized text. Underscore counts as a word
    // character, so identifiers like updated_at or pragma_version never match.
    private static readonly Regex ForbiddenOperationRegex = new(
        @"\b(DELETE|UPDATE|INSERT|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|MERGE|EXEC|EXECUTE|CALL|ATTACH|DETACH|PRAGMA|COPY|VACUUM|REINDEX|LOAD|INTO|OUTFILE|DUMPFILE|OPENROWSET|OPENQUERY)\b|\b(?:XP|SP)_\w+",
        RegexOptions.Compiled);

    private static readonly Regex StartsWithSelectOrWithRegex = new(
        @"^(SELECT|WITH)\b",
        RegexOptions.Compiled);

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

        // A query is only safe if it is safe under BOTH block-comment dialect
        // interpretations (nesting: PostgreSQL/SQL Server; non-nesting: MySQL/SQLite/Athena).
        var nested = SqlNormalizer.Normalize(sql, nestedBlockComments: true);
        var error = ValidateNormalized(nested, additionalForbidden);
        if (error != null)
            return error;

        var flat = SqlNormalizer.Normalize(sql, nestedBlockComments: false);
        if (flat != nested)
        {
            error = ValidateNormalized(flat, additionalForbidden);
            if (error != null)
                return error;
        }

        // Tenant isolation guard (presence check; structural verification is layered
        // on top by TenantFilterVerifier at the engine level).
        if (!string.IsNullOrEmpty(tenantIdColumn) && !string.IsNullOrEmpty(tenantId))
        {
            if (!sql.Contains(tenantId, StringComparison.OrdinalIgnoreCase))
                return $"Query must contain a filter on tenant column '{tenantIdColumn}'.";
        }

        return null; // Valid
    }

    private static string? ValidateNormalized(string normalized, IEnumerable<string>? additionalForbidden)
    {
        var upper = normalized.ToUpperInvariant();

        // Must start with SELECT or WITH (CTE)
        if (!StartsWithSelectOrWithRegex.IsMatch(upper))
            return "Only SELECT queries are allowed.";

        // Multi-statement detection — literals and comments are already stripped,
        // so any interior semicolon is a real statement separator.
        var semicolonIndex = upper.IndexOf(';');
        if (semicolonIndex >= 0 && semicolonIndex < upper.TrimEnd(';', ' ').Length)
            return "Multiple SQL statements are not allowed.";

        // Dangerous-operation denylist (word-boundary)
        var match = ForbiddenOperationRegex.Match(upper);
        if (match.Success)
            return $"Forbidden SQL keyword detected: {match.Value}";

        if (additionalForbidden != null)
        {
            foreach (var keyword in additionalForbidden)
            {
                if (upper.Contains(keyword.ToUpperInvariant()))
                    return $"Forbidden SQL keyword detected: {keyword.Trim()}";
            }
        }

        return null;
    }
}

using System.Text;
using System.Text.RegularExpressions;

namespace NaturalQuery.Validation;

/// <summary>
/// Structurally verifies that the tenant value is applied as a real equality filter
/// on the configured tenant column — not merely present somewhere in the query text,
/// a comment, or an unrelated string literal (FR-007).
/// </summary>
public static class TenantFilterVerifier
{
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Returns true when the query contains <c>[alias.]tenantColumn = 'tenantId'</c>
    /// (case-insensitive, whitespace/comment tolerant) outside comments and literals.
    /// </summary>
    public static bool HasTenantFilter(string sql, string tenantIdColumn, string tenantId)
    {
        if (string.IsNullOrWhiteSpace(sql) ||
            string.IsNullOrEmpty(tenantIdColumn) ||
            string.IsNullOrEmpty(tenantId))
            return false;

        // Strip comments, collapse whitespace, and empty every string literal EXCEPT
        // the ones whose exact content is the tenant ID. This prevents a crafted
        // larger literal from being mistaken for a real filter.
        var normalized = NormalizeKeepingLiteral(sql, tenantId);

        // \b guards against substring matches (other_tenant_id must not satisfy tenant_id);
        // an alias qualifier like "u." still leaves a word boundary before the column.
        var pattern = $@"\b{Regex.Escape(tenantIdColumn)}\b\s*=\s*'{Regex.Escape(tenantId)}'";

        try
        {
            return Regex.IsMatch(normalized, pattern, RegexOptions.IgnoreCase, MatchTimeout);
        }
        catch (RegexMatchTimeoutException)
        {
            return false; // fail closed
        }
    }

    /// <summary>
    /// Comment-stripping, whitespace-collapsing normalization that preserves only the
    /// string literals exactly equal to <paramref name="literalToKeep"/>; every other
    /// literal is emptied to <c>''</c>.
    /// </summary>
    private static string NormalizeKeepingLiteral(string sql, string literalToKeep)
    {
        var sb = new StringBuilder(sql.Length);
        var lastWasSpace = true;
        var i = 0;

        void AppendSpace()
        {
            if (!lastWasSpace)
            {
                sb.Append(' ');
                lastWasSpace = true;
            }
        }

        while (i < sql.Length)
        {
            var c = sql[i];

            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n') i++;
                AppendSpace();
                continue;
            }

            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                i += 2;
                var depth = 1;
                while (i < sql.Length && depth > 0)
                {
                    if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*') { depth++; i += 2; }
                    else if (sql[i] == '*' && i + 1 < sql.Length && sql[i + 1] == '/') { depth--; i += 2; }
                    else i++;
                }
                AppendSpace();
                continue;
            }

            if (c == '\'')
            {
                i++;
                var content = new StringBuilder();
                while (i < sql.Length)
                {
                    if (sql[i] == '\'')
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            content.Append('\'');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    content.Append(sql[i]);
                    i++;
                }

                sb.Append('\'');
                if (content.ToString() == literalToKeep)
                    sb.Append(literalToKeep);
                sb.Append('\'');
                lastWasSpace = false;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                AppendSpace();
                i++;
                continue;
            }

            sb.Append(c);
            lastWasSpace = false;
            i++;
        }

        return sb.ToString();
    }
}

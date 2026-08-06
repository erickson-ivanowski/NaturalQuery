using System.Text;

namespace NaturalQuery.Validation;

/// <summary>
/// Normalizes SQL text for safety evaluation: strips comments, empties string
/// literals (quote-escape aware), and collapses all whitespace to single spaces —
/// so obfuscation (comments, line breaks, casing tricks) cannot hide dangerous operations.
/// </summary>
public static class SqlNormalizer
{
    /// <summary>
    /// Normalizes SQL for safety analysis.
    /// </summary>
    /// <param name="sql">Raw SQL text.</param>
    /// <param name="nestedBlockComments">
    /// True (default) treats block comments as nesting (PostgreSQL / SQL Server semantics);
    /// false ends a block comment at the first terminator (MySQL / SQLite / Athena semantics).
    /// Validators should check both interpretations — a query is only safe if it is safe under each.
    /// </param>
    public static string Normalize(string sql, bool nestedBlockComments = true)
    {
        if (string.IsNullOrEmpty(sql))
            return string.Empty;

        var sb = new StringBuilder(sql.Length);
        var lastWasSpace = true; // suppresses leading whitespace
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

            // Line comment: -- to end of line
            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n') i++;
                AppendSpace();
                continue;
            }

            // Block comment
            if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
            {
                i += 2;
                var depth = 1;
                while (i < sql.Length && depth > 0)
                {
                    if (nestedBlockComments && sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                    {
                        depth++;
                        i += 2;
                    }
                    else if (sql[i] == '*' && i + 1 < sql.Length && sql[i + 1] == '/')
                    {
                        depth--;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }
                }
                AppendSpace();
                continue;
            }

            // String literal: empty it, honoring '' escapes
            if (c == '\'')
            {
                i++;
                while (i < sql.Length)
                {
                    if (sql[i] == '\'')
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == '\'')
                        {
                            i += 2; // escaped quote inside literal
                            continue;
                        }
                        i++; // closing quote
                        break;
                    }
                    i++;
                }
                sb.Append("''");
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

        // Trim trailing space
        while (sb.Length > 0 && sb[^1] == ' ')
            sb.Length--;

        return sb.ToString();
    }
}

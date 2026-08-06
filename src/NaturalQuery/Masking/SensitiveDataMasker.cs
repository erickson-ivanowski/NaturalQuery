using System.Text.RegularExpressions;
using NaturalQuery.Models;

namespace NaturalQuery.Masking;

/// <summary>
/// Fully redacts (***) the values of columns marked Sensitive in the schema
/// configuration, across every output form: table data and chart data (and,
/// by extension, every export produced from the masked result).
/// </summary>
public static class SensitiveDataMasker
{
    /// <summary>Replacement for sensitive values in all output forms.</summary>
    public const string Redaction = "***";

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Masks sensitive-column values in place. Table rows are matched by column
    /// name (case-insensitive). Chart labels are masked when the query selects a
    /// sensitive column as the chart label expression.
    /// </summary>
    public static void Mask(QueryResult result, IEnumerable<TableSchema> tables)
    {
        var sensitiveColumns = new HashSet<string>(
            tables.SelectMany(t => t.Columns).Where(c => c.Sensitive).Select(c => c.Name),
            StringComparer.OrdinalIgnoreCase);

        if (sensitiveColumns.Count == 0)
            return;

        if (result.TableData != null)
        {
            foreach (var row in result.TableData)
            {
                foreach (var key in row.Keys.Where(sensitiveColumns.Contains).ToList())
                    row[key] = Redaction;
            }
        }

        if (result.ChartData is { Count: > 0 } && LabelComesFromSensitiveColumn(result.Sql, sensitiveColumns))
        {
            result.ChartData = result.ChartData
                .Select(p => p with { Label = Redaction })
                .ToList();
        }
    }

    /// <summary>
    /// Chart queries follow the generated shape "SELECT &lt;expr&gt; AS label, ...".
    /// The labels are sensitive when a sensitive column feeds that label expression.
    /// </summary>
    private static bool LabelComesFromSensitiveColumn(string sql, IReadOnlyCollection<string> sensitiveColumns)
    {
        if (string.IsNullOrEmpty(sql))
            return false;

        foreach (var column in sensitiveColumns)
        {
            var pattern = $@"\b{Regex.Escape(column)}\b[^,]{{0,40}}?\bAS\s+label\b";
            try
            {
                if (Regex.IsMatch(sql, pattern, RegexOptions.IgnoreCase, MatchTimeout))
                    return true;
            }
            catch (RegexMatchTimeoutException)
            {
                return true; // fail closed: mask rather than leak
            }
        }

        return false;
    }
}

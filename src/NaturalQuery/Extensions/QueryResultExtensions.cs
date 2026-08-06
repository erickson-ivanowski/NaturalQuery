using System.Text;
using System.Text.Json;
using NaturalQuery.Export;
using NaturalQuery.Models;

namespace NaturalQuery.Extensions;

/// <summary>
/// Extension methods for exporting QueryResult data to common formats.
/// </summary>
public static class QueryResultExtensions
{
    /// <summary>
    /// Exports the query result data to CSV format.
    /// For chart data, exports label and value columns.
    /// For table data, exports all columns.
    /// </summary>
    public static string ToCsv(this QueryResult result)
    {
        var sb = new StringBuilder();

        if (result.TableData?.Count > 0)
        {
            // Get all unique column names from all rows
            var columns = result.TableData
                .SelectMany(r => r.Keys)
                .Distinct()
                .ToList();

            // Header
            sb.AppendLine(string.Join(",", columns.Select(EscapeCsvField)));

            // Rows
            foreach (var row in result.TableData)
            {
                var values = columns.Select(col =>
                    row.TryGetValue(col, out var val) ? EscapeCsvField(val) : "");
                sb.AppendLine(string.Join(",", values));
            }
        }
        else if (result.ChartData?.Count > 0)
        {
            sb.AppendLine("Label,Value");
            foreach (var point in result.ChartData)
            {
                sb.AppendLine($"{EscapeCsvField(point.Label)},{point.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Exports the query result data to a JSON string.
    /// Includes metadata (title, description, chart type) along with the data.
    /// </summary>
    public static string ToJson(this QueryResult result, bool indented = true)
    {
        var options = new JsonSerializerOptions
        {
            WriteIndented = indented,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        var export = new
        {
            result.Title,
            result.Description,
            result.ChartType,
            result.Sql,
            result.Suggestions,
            result.TokensUsed,
            result.ElapsedMs,
            Data = result.TableData as object ?? result.ChartData
        };

        return JsonSerializer.Serialize(export, options);
    }

    /// <summary>
    /// Exports the query result data to an Excel (.xlsx) file, alongside the
    /// existing CSV/JSON exports. Table data exports all columns; chart data
    /// exports Label/Value columns.
    /// </summary>
    public static byte[] ToExcel(this QueryResult result)
    {
        var (headers, rows) = BuildTable(result);
        return MinimalXlsxWriter.Write(headers, rows);
    }

    /// <summary>Exports the query result to an Excel (.xlsx) stream.</summary>
    public static Stream ToExcelStream(this QueryResult result) => new MemoryStream(result.ToExcel());

    private static (List<string> Headers, List<IReadOnlyList<string>> Rows) BuildTable(QueryResult result)
    {
        if (result.TableData?.Count > 0)
        {
            var columns = result.TableData.SelectMany(r => r.Keys).Distinct().ToList();
            var rows = result.TableData
                .Select(row => (IReadOnlyList<string>)columns.Select(c => row.TryGetValue(c, out var v) ? v : "").ToList())
                .ToList();
            return (columns, rows);
        }

        if (result.ChartData?.Count > 0)
        {
            var rows = result.ChartData
                .Select(p => (IReadOnlyList<string>)new List<string> { p.Label, p.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) })
                .ToList();
            return (new List<string> { "Label", "Value" }, rows);
        }

        return (new List<string>(), new List<IReadOnlyList<string>>());
    }

    private static string EscapeCsvField(string field)
    {
        if (string.IsNullOrEmpty(field)) return "";
        if (field.Contains(',') || field.Contains('"') || field.Contains('\n') || field.Contains('\r'))
        {
            return $"\"{field.Replace("\"", "\"\"")}\"";
        }
        return field;
    }
}

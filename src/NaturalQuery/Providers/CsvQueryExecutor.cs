using System.Globalization;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// Query executor that loads CSV data into an in-memory SQLite database.
/// Enables natural language queries on CSV files without a database setup.
/// The CSV is parsed once on first query, then subsequent queries run against the in-memory database.
/// </summary>
public class CsvQueryExecutor : IQueryExecutor, IDisposable
{
    private readonly string _csvPath;
    private readonly Stream? _csvStream;
    private readonly string _tableName;
    private readonly int _maxResultRows;
    private readonly ILogger<CsvQueryExecutor> _logger;
    private SqliteConnection? _connection;
    private readonly object _initLock = new();
    private bool _initialized;
    private TableSchema? _discoveredSchema;

    /// <summary>
    /// Creates a CSV query executor from a file path.
    /// </summary>
    public CsvQueryExecutor(string csvPath, ILogger<CsvQueryExecutor> logger, string tableName = "data", int maxResultRows = 0)
    {
        _csvPath = csvPath;
        _tableName = tableName;
        _logger = logger;
        _maxResultRows = maxResultRows;
    }

    /// <summary>
    /// Creates a CSV query executor from a stream.
    /// </summary>
    public CsvQueryExecutor(Stream csvStream, ILogger<CsvQueryExecutor> logger, string tableName = "data", int maxResultRows = 0)
    {
        _csvPath = "";
        _csvStream = csvStream;
        _tableName = tableName;
        _logger = logger;
        _maxResultRows = maxResultRows;
    }

    /// <inheritdoc />
    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct = default)
    {
        EnsureInitialized();

        _logger.LogInformation("[CSV] Executing chart query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<DataPoint>();

        await using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var label = reader.GetValue(0)?.ToString() ?? "";
            var rawValue = reader.GetValue(reader.FieldCount - 1);

            if (rawValue != null && double.TryParse(
                    Convert.ToString(rawValue, CultureInfo.InvariantCulture),
                    NumberStyles.Any,
                    CultureInfo.InvariantCulture, out var value))
            {
                results.Add(new DataPoint(label, value));
            }

            if (_maxResultRows > 0 && results.Count > _maxResultRows)
                break;
        }

        _logger.LogInformation("[CSV] Chart query returned {Count} data points", results.Count);
        return results;
    }

    /// <inheritdoc />
    public async Task<List<Dictionary<string, string>>> ExecuteTableQueryAsync(string sql, CancellationToken ct = default)
    {
        EnsureInitialized();

        _logger.LogInformation("[CSV] Executing table query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<Dictionary<string, string>>();

        await using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var row = new Dictionary<string, string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.GetValue(i)?.ToString() ?? "";
            }
            results.Add(row);

            if (_maxResultRows > 0 && results.Count > _maxResultRows)
                break;
        }

        _logger.LogInformation("[CSV] Table query returned {Count} rows", results.Count);
        return results;
    }

    /// <summary>
    /// Returns the auto-discovered table schema from the CSV headers and detected types.
    /// Useful for configuring NaturalQueryOptions.Tables after loading a CSV.
    /// </summary>
    public TableSchema GetDiscoveredSchema()
    {
        EnsureInitialized();
        return _discoveredSchema ?? new TableSchema(_tableName, Array.Empty<ColumnDef>());
    }

    private void EnsureInitialized()
    {
        if (_initialized) return;
        lock (_initLock)
        {
            if (_initialized) return;
            InitializeDatabase();
            _initialized = true;
        }
    }

    private void InitializeDatabase()
    {
        _logger.LogInformation("[CSV] Loading CSV into in-memory SQLite database");

        // Read CSV lines
        string[] lines;
        if (_csvStream != null)
        {
            using var reader = new StreamReader(_csvStream);
            lines = reader.ReadToEnd().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        }
        else
        {
            lines = File.ReadAllLines(_csvPath);
        }

        if (lines.Length < 2)
        {
            _logger.LogWarning("[CSV] CSV file has less than 2 lines (header + data)");
            _connection = new SqliteConnection("DataSource=:memory:");
            _connection.Open();
            _discoveredSchema = new TableSchema(_tableName, Array.Empty<ColumnDef>());
            return;
        }

        // Parse header
        var headers = ParseCsvLine(lines[0]);
        var sanitizedHeaders = headers.Select(SanitizeColumnName).ToArray();

        // Parse data rows
        var dataRows = new List<string[]>();
        for (var i = 1; i < lines.Length; i++)
        {
            var row = ParseCsvLine(lines[i]);
            if (row.Length > 0)
                dataRows.Add(row);
        }

        // Detect column types from data
        var columnTypes = new string[sanitizedHeaders.Length];
        for (var col = 0; col < sanitizedHeaders.Length; col++)
        {
            columnTypes[col] = DetectColumnType(dataRows, col);
        }

        // Create SQLite database (single in-memory connection, kept alive)
        _connection = new SqliteConnection("DataSource=:memory:");
        _connection.Open();

        // Build CREATE TABLE
        var columnDefs = new List<string>();
        var schemaCols = new List<ColumnDef>();
        for (var i = 0; i < sanitizedHeaders.Length; i++)
        {
            var sqliteType = columnTypes[i] switch
            {
                "int" => "INTEGER",
                "double" => "REAL",
                _ => "TEXT"
            };
            columnDefs.Add($"\"{sanitizedHeaders[i]}\" {sqliteType}");
            schemaCols.Add(new ColumnDef(sanitizedHeaders[i], columnTypes[i]));
        }

        using var createCmd = _connection.CreateCommand();
        createCmd.CommandText = $"CREATE TABLE \"{_tableName}\" ({string.Join(", ", columnDefs)})";
        createCmd.ExecuteNonQuery();

        // Insert data
        foreach (var row in dataRows)
        {
            using var insertCmd = _connection.CreateCommand();
            var placeholders = new List<string>();
            for (var i = 0; i < Math.Min(row.Length, sanitizedHeaders.Length); i++)
            {
                var paramName = $"@p{i}";
                placeholders.Add(paramName);
                insertCmd.Parameters.AddWithValue(paramName, string.IsNullOrEmpty(row[i]) ? (object)DBNull.Value : row[i]);
            }
            // Fill missing columns with NULL
            for (var i = row.Length; i < sanitizedHeaders.Length; i++)
            {
                placeholders.Add($"@p{i}");
                insertCmd.Parameters.AddWithValue($"@p{i}", DBNull.Value);
            }
            insertCmd.CommandText = $"INSERT INTO \"{_tableName}\" VALUES ({string.Join(", ", placeholders)})";
            insertCmd.ExecuteNonQuery();
        }

        _discoveredSchema = new TableSchema(_tableName, schemaCols);

        _logger.LogInformation("[CSV] Loaded {Rows} rows, {Cols} columns into table '{Table}'",
            dataRows.Count, sanitizedHeaders.Length, _tableName);
    }

    private static string DetectColumnType(List<string[]> rows, int colIndex)
    {
        var allInt = true;
        var allDouble = true;
        var hasValues = false;

        foreach (var row in rows)
        {
            if (colIndex >= row.Length || string.IsNullOrWhiteSpace(row[colIndex]))
                continue;

            hasValues = true;
            var val = row[colIndex].Trim();

            if (!int.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                allInt = false;
            if (!double.TryParse(val, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
                allDouble = false;

            if (!allInt && !allDouble)
                break;
        }

        if (!hasValues) return "string";
        if (allInt) return "int";
        if (allDouble) return "double";
        return "string";
    }

    private static string[] ParseCsvLine(string line)
    {
        var result = new List<string>();
        var current = new System.Text.StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == ',' && !inQuotes)
            {
                result.Add(current.ToString().Trim());
                current.Clear();
            }
            else if (c == '\r')
            {
                // Skip carriage return
            }
            else
            {
                current.Append(c);
            }
        }
        result.Add(current.ToString().Trim());
        return result.ToArray();
    }

    private static string SanitizeColumnName(string name)
    {
        var sanitized = name.Trim().Trim('"').Trim();
        // Replace non-alphanumeric chars with underscore
        return System.Text.RegularExpressions.Regex.Replace(sanitized, @"[^a-zA-Z0-9_]", "_").ToLowerInvariant();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _connection?.Close();
        _connection?.Dispose();
    }
}

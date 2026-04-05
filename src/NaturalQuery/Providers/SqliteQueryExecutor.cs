using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// Query executor for SQLite databases using Microsoft.Data.Sqlite.
/// Lightweight and ideal for testing, prototyping, and embedded applications.
/// </summary>
public class SqliteQueryExecutor : IQueryExecutor
{
    private readonly string _connectionString;
    private readonly int _timeoutSeconds;
    private readonly ILogger<SqliteQueryExecutor> _logger;

    /// <summary>
    /// Initializes the SQLite query executor.
    /// </summary>
    /// <param name="connectionString">SQLite connection string (e.g., "Data Source=mydb.sqlite").</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds. Default: 30.</param>
    public SqliteQueryExecutor(
        string connectionString,
        ILogger<SqliteQueryExecutor> logger,
        int timeoutSeconds = 30)
    {
        _connectionString = connectionString;
        _logger = logger;
        _timeoutSeconds = timeoutSeconds;
    }

    /// <inheritdoc />
    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("[SQLite] Executing chart query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<DataPoint>();

        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _timeoutSeconds;

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var label = reader.GetValue(0)?.ToString() ?? "";
            var rawValue = reader.GetValue(reader.FieldCount - 1);

            if (rawValue != null && double.TryParse(
                Convert.ToString(rawValue, System.Globalization.CultureInfo.InvariantCulture),
                System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                results.Add(new DataPoint(label, value));
            }
        }

        _logger.LogInformation("[SQLite] Chart query returned {Count} data points", results.Count);
        return results;
    }

    /// <inheritdoc />
    public async Task<List<Dictionary<string, string>>> ExecuteTableQueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("[SQLite] Executing table query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<Dictionary<string, string>>();

        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _timeoutSeconds;

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var row = new Dictionary<string, string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.GetValue(i)?.ToString() ?? "";
            }
            results.Add(row);
        }

        _logger.LogInformation("[SQLite] Table query returned {Count} rows", results.Count);
        return results;
    }
}

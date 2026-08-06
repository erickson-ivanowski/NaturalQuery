using Microsoft.Extensions.Logging;
using MySqlConnector;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// Query executor for MySQL / MariaDB using MySqlConnector.
/// Supports optional transaction wrapping (BEGIN + ROLLBACK) and result row capping,
/// with the same safety guarantees as the other executors.
/// </summary>
public class MySqlQueryExecutor : IQueryExecutor
{
    private readonly string _connectionString;
    private readonly int _timeoutSeconds;
    private readonly bool _wrapInTransaction;
    private readonly int _maxResultRows;
    private readonly ILogger<MySqlQueryExecutor> _logger;

    /// <summary>
    /// Initializes the MySQL/MariaDB query executor.
    /// </summary>
    /// <param name="connectionString">MySQL connection string.</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds. Default: 30.</param>
    /// <param name="wrapInTransaction">
    /// When true, wraps every query in a transaction rolled back at the end — an extra
    /// safety layer against accidental writes. Default: false.
    /// </param>
    /// <param name="maxResultRows">Result row cap; 0 disables. Default: 0.</param>
    public MySqlQueryExecutor(
        string connectionString,
        ILogger<MySqlQueryExecutor> logger,
        int timeoutSeconds = 30,
        bool wrapInTransaction = false,
        int maxResultRows = 0)
    {
        _connectionString = connectionString;
        _logger = logger;
        _timeoutSeconds = timeoutSeconds;
        _wrapInTransaction = wrapInTransaction;
        _maxResultRows = maxResultRows;
    }

    /// <inheritdoc />
    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("[MySQL] Executing chart query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<DataPoint>();

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        MySqlTransaction? tx = null;
        if (_wrapInTransaction)
            tx = await conn.BeginTransactionAsync(ct);

        try
        {
            await using var cmd = new MySqlCommand(sql, conn, tx) { CommandTimeout = _timeoutSeconds };
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

                if (_maxResultRows > 0 && results.Count > _maxResultRows)
                    break;
            }

            _logger.LogInformation("[MySQL] Chart query returned {Count} data points", results.Count);
            return results;
        }
        finally
        {
            if (tx != null)
            {
                await tx.RollbackAsync(ct);
                await tx.DisposeAsync();
            }
        }
    }

    /// <inheritdoc />
    public async Task<List<Dictionary<string, string>>> ExecuteTableQueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("[MySQL] Executing table query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<Dictionary<string, string>>();

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        MySqlTransaction? tx = null;
        if (_wrapInTransaction)
            tx = await conn.BeginTransactionAsync(ct);

        try
        {
            await using var cmd = new MySqlCommand(sql, conn, tx) { CommandTimeout = _timeoutSeconds };
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

            _logger.LogInformation("[MySQL] Table query returned {Count} rows", results.Count);
            return results;
        }
        finally
        {
            if (tx != null)
            {
                await tx.RollbackAsync(ct);
                await tx.DisposeAsync();
            }
        }
    }
}

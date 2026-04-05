using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// Query executor for Microsoft SQL Server databases using Microsoft.Data.SqlClient.
/// Supports optional transaction wrapping for extra safety (BEGIN + ROLLBACK).
/// </summary>
public class SqlServerQueryExecutor : IQueryExecutor
{
    private readonly string _connectionString;
    private readonly int _timeoutSeconds;
    private readonly bool _wrapInTransaction;
    private readonly ILogger<SqlServerQueryExecutor> _logger;

    /// <summary>
    /// Initializes the SQL Server query executor.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="logger">Logger instance.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds. Default: 30.</param>
    /// <param name="wrapInTransaction">
    /// When true, wraps every query in BEGIN TRANSACTION + ROLLBACK as an extra safety layer.
    /// This prevents any accidental writes even if SQL validation is bypassed.
    /// Basically free for SELECT queries. Default: false.
    /// </param>
    public SqlServerQueryExecutor(
        string connectionString,
        ILogger<SqlServerQueryExecutor> logger,
        int timeoutSeconds = 30,
        bool wrapInTransaction = false)
    {
        _connectionString = connectionString;
        _logger = logger;
        _timeoutSeconds = timeoutSeconds;
        _wrapInTransaction = wrapInTransaction;
    }

    /// <inheritdoc />
    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct = default)
    {
        _logger.LogInformation("[SqlServer] Executing chart query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<DataPoint>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        SqlTransaction? tx = null;
        if (_wrapInTransaction)
            tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);

        try
        {
            await using var cmd = new SqlCommand(sql, conn, tx) { CommandTimeout = _timeoutSeconds };
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

            _logger.LogInformation("[SqlServer] Chart query returned {Count} data points", results.Count);
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
        _logger.LogInformation("[SqlServer] Executing table query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var results = new List<Dictionary<string, string>>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        SqlTransaction? tx = null;
        if (_wrapInTransaction)
            tx = (SqlTransaction)await conn.BeginTransactionAsync(ct);

        try
        {
            await using var cmd = new SqlCommand(sql, conn, tx) { CommandTimeout = _timeoutSeconds };
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

            _logger.LogInformation("[SqlServer] Table query returned {Count} rows", results.Count);
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

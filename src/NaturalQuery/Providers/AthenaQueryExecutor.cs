using Amazon.Athena;
using Amazon.Athena.Model;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// Query executor using Amazon Athena.
/// </summary>
public class AthenaQueryExecutor : IQueryExecutor
{
    private readonly IAmazonAthena _athenaClient;
    private readonly string _database;
    private readonly string _workgroup;
    private readonly string _outputLocation;
    private readonly ILogger<AthenaQueryExecutor> _logger;
    private readonly int _timeoutSeconds;

    private readonly int _maxResultRows;

    public AthenaQueryExecutor(
        IAmazonAthena athenaClient,
        string database,
        string workgroup,
        string outputLocation,
        ILogger<AthenaQueryExecutor> logger,
        int timeoutSeconds = 30,
        int maxResultRows = 0)
    {
        _athenaClient = athenaClient;
        _database = database;
        _workgroup = workgroup;
        _outputLocation = outputLocation;
        _logger = logger;
        _timeoutSeconds = timeoutSeconds;
        _maxResultRows = maxResultRows;
    }

    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct = default)
    {
        var rows = await ExecuteAndGetRowsAsync(sql, ct);

        var results = new List<DataPoint>();
        foreach (var row in rows.Skip(1)) // Skip header
        {
            if (row.Data.Count < 2) continue;
            var label = row.Data[0].VarCharValue ?? "";
            if (double.TryParse(row.Data[^1].VarCharValue, System.Globalization.NumberStyles.Any,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                results.Add(new DataPoint(label, value));
            }

            if (_maxResultRows > 0 && results.Count > _maxResultRows)
                break;
        }

        return results;
    }

    public async Task<List<Dictionary<string, string>>> ExecuteTableQueryAsync(string sql, CancellationToken ct = default)
    {
        var rows = await ExecuteAndGetRowsAsync(sql, ct);

        if (rows.Count == 0) return new List<Dictionary<string, string>>();

        // First row = headers
        var headers = rows[0].Data.Select(d => d.VarCharValue ?? "").ToList();

        var results = new List<Dictionary<string, string>>();
        foreach (var row in rows.Skip(1))
        {
            var dict = new Dictionary<string, string>();
            for (var i = 0; i < Math.Min(headers.Count, row.Data.Count); i++)
            {
                dict[headers[i]] = row.Data[i].VarCharValue ?? "";
            }
            results.Add(dict);

            if (_maxResultRows > 0 && results.Count > _maxResultRows)
                break;
        }

        return results;
    }

    private async Task<List<Row>> ExecuteAndGetRowsAsync(string sql, CancellationToken ct)
    {
        _logger.LogInformation("[Athena] Executing query: {Sql}", sql[..Math.Min(200, sql.Length)]);

        var startResponse = await _athenaClient.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = sql,
            QueryExecutionContext = new QueryExecutionContext { Database = _database },
            WorkGroup = _workgroup,
            ResultConfiguration = new ResultConfiguration { OutputLocation = _outputLocation }
        }, ct);

        var queryId = startResponse.QueryExecutionId;
        var elapsed = 0;

        while (elapsed < _timeoutSeconds * 1000)
        {
            await Task.Delay(1000, ct);
            elapsed += 1000;

            var statusResponse = await _athenaClient.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = queryId
            }, ct);

            var state = statusResponse.QueryExecution.Status.State;

            if (state == QueryExecutionState.SUCCEEDED)
            {
                var resultResponse = await _athenaClient.GetQueryResultsAsync(new GetQueryResultsRequest
                {
                    QueryExecutionId = queryId
                }, ct);

                _logger.LogInformation("[Athena] Query succeeded. Rows: {Count}", resultResponse.ResultSet.Rows.Count - 1);
                return resultResponse.ResultSet.Rows;
            }

            if (state == QueryExecutionState.FAILED)
            {
                var reason = statusResponse.QueryExecution.Status.StateChangeReason;
                _logger.LogError("[Athena] Query failed: {Reason}", reason);
                throw new InvalidOperationException($"Query failed: {reason}");
            }

            if (state == QueryExecutionState.CANCELLED)
            {
                throw new InvalidOperationException("Query was cancelled.");
            }
        }

        // Timeout — stop the query
        await _athenaClient.StopQueryExecutionAsync(new StopQueryExecutionRequest
        {
            QueryExecutionId = queryId
        }, ct);

        throw new TimeoutException($"Query exceeded {_timeoutSeconds}s timeout.");
    }
}

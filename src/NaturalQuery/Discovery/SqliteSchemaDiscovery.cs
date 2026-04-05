using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Discovery;

/// <summary>
/// Discovers table schemas from a SQLite database using PRAGMA table_info.
/// </summary>
public class SqliteSchemaDiscovery : ISchemaDiscovery
{
    private readonly string _connectionString;
    private readonly ILogger<SqliteSchemaDiscovery> _logger;

    /// <summary>
    /// Initializes SQLite schema discovery.
    /// </summary>
    /// <param name="connectionString">SQLite connection string (e.g., "Data Source=mydb.sqlite").</param>
    /// <param name="logger">Logger instance.</param>
    public SqliteSchemaDiscovery(string connectionString, ILogger<SqliteSchemaDiscovery> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    /// <inheritdoc />
    /// <remarks>
    /// The <paramref name="schemaFilter"/> parameter is ignored for SQLite as it does not support schemas.
    /// Tables are discovered from sqlite_master, excluding internal sqlite_ system tables.
    /// </remarks>
    public async Task<List<TableSchema>> DiscoverAsync(string? schemaFilter = null, CancellationToken ct = default)
    {
        _logger.LogInformation("[SchemaDiscovery] Discovering tables in SQLite database");

        var tables = new List<TableSchema>();

        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Step 1: Get all user table names from sqlite_master
        var tableNames = new List<string>();

        await using (var tableCmd = conn.CreateCommand())
        {
            tableCmd.CommandText = @"
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY name";

            await using var tableReader = await tableCmd.ExecuteReaderAsync(ct);

            while (await tableReader.ReadAsync(ct))
            {
                tableNames.Add(tableReader.GetString(0));
            }
        }

        // Step 2: For each table, get column info via PRAGMA table_info
        foreach (var tableName in tableNames)
        {
            var schema = new TableSchema { Name = tableName };

            await using var colCmd = conn.CreateCommand();
            // PRAGMA does not support parameters, so we validate the table name
            // against the list we already retrieved from sqlite_master (safe from injection).
            colCmd.CommandText = $"PRAGMA table_info(\"{tableName.Replace("\"", "\"\"")}\")";

            await using var colReader = await colCmd.ExecuteReaderAsync(ct);

            // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            while (await colReader.ReadAsync(ct))
            {
                var columnName = colReader.GetString(1);
                var dataType = colReader.IsDBNull(2) ? "" : colReader.GetString(2);
                var notNull = colReader.GetInt64(3) == 1;

                var mappedType = MapSqliteType(dataType);
                var desc = notNull ? null : "nullable";

                schema.Columns.Add(new ColumnDef(columnName, mappedType, desc));
            }

            tables.Add(schema);
        }

        _logger.LogInformation("[SchemaDiscovery] Discovered {Count} tables", tables.Count);

        return tables;
    }

    /// <summary>
    /// Maps a SQLite data type to a simplified NaturalQuery type.
    /// </summary>
    /// <param name="sqliteType">The SQLite column type declaration.</param>
    /// <returns>A simplified type string (int, double, boolean, or string).</returns>
    private static string MapSqliteType(string sqliteType)
    {
        var upper = sqliteType.ToUpperInvariant();

        if (upper.Contains("INT"))
            return "int";

        if (upper.Contains("REAL") || upper.Contains("FLOAT") || upper.Contains("DOUBLE"))
            return "double";

        if (upper.Contains("BOOL"))
            return "boolean";

        if (upper.Contains("TEXT") || upper.Contains("VARCHAR") || upper.Contains("CHAR") || upper.Contains("CLOB"))
            return "string";

        if (upper.Contains("BLOB"))
            return "string";

        // SQLite type affinity: empty or unrecognized types default to string
        return "string";
    }
}

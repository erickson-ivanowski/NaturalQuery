using Microsoft.Extensions.Logging;
using MySqlConnector;
using NaturalQuery.Models;

namespace NaturalQuery.Discovery;

/// <summary>
/// Discovers table schemas from a MySQL / MariaDB database using information_schema.
/// </summary>
public class MySqlSchemaDiscovery : ISchemaDiscovery
{
    private readonly string _connectionString;
    private readonly ILogger<MySqlSchemaDiscovery> _logger;

    /// <summary>
    /// Initializes MySQL schema discovery.
    /// </summary>
    /// <param name="connectionString">MySQL connection string.</param>
    /// <param name="logger">Logger instance.</param>
    public MySqlSchemaDiscovery(string connectionString, ILogger<MySqlSchemaDiscovery> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<List<TableSchema>> DiscoverAsync(string? schemaFilter = null, CancellationToken ct = default)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Default to the connection's own database when no schema filter is given.
        var schema = schemaFilter ?? conn.Database;

        _logger.LogInformation("[SchemaDiscovery] Discovering tables in schema '{Schema}'", schema);

        var tables = new Dictionary<string, TableSchema>();

        const string sql = @"
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema
            ORDER BY TABLE_NAME, ORDINAL_POSITION";

        await using var cmd = new MySqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@schema", schema);

        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var tableName = reader.GetString(0);
            var columnName = reader.GetString(1);
            var dataType = reader.GetString(2);
            var isNullable = reader.GetString(3);

            if (!tables.ContainsKey(tableName))
                tables[tableName] = new TableSchema { Name = tableName };

            var mappedType = MapMySqlType(dataType);
            var desc = isNullable == "YES" ? "nullable" : null;

            tables[tableName].Columns.Add(new ColumnDef(columnName, mappedType, desc));
        }

        _logger.LogInformation("[SchemaDiscovery] Discovered {Count} tables", tables.Count);

        return tables.Values.ToList();
    }

    private static string MapMySqlType(string mySqlType) => mySqlType.ToLowerInvariant() switch
    {
        "int" or "bigint" or "smallint" or "tinyint" or "mediumint" => "int",
        "decimal" or "numeric" or "float" or "double" or "dec" => "double",
        "bit" or "bool" or "boolean" => "boolean",
        "date" => "date",
        "datetime" or "timestamp" => "timestamp",
        "varchar" or "char" or "text" or "tinytext" or "mediumtext" or "longtext" or "enum" or "set" => "string",
        _ => "string"
    };
}

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Discovery;

/// <summary>
/// Discovers table schemas from a Microsoft SQL Server database using INFORMATION_SCHEMA.
/// </summary>
public class SqlServerSchemaDiscovery : ISchemaDiscovery
{
    private readonly string _connectionString;
    private readonly ILogger<SqlServerSchemaDiscovery> _logger;

    /// <summary>
    /// Initializes SQL Server schema discovery.
    /// </summary>
    /// <param name="connectionString">SQL Server connection string.</param>
    /// <param name="logger">Logger instance.</param>
    public SqlServerSchemaDiscovery(string connectionString, ILogger<SqlServerSchemaDiscovery> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<List<TableSchema>> DiscoverAsync(string? schemaFilter = null, CancellationToken ct = default)
    {
        var schema = schemaFilter ?? "dbo";

        _logger.LogInformation("[SchemaDiscovery] Discovering tables in schema '{Schema}'", schema);

        var tables = new Dictionary<string, TableSchema>();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var sql = @"
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema
            ORDER BY TABLE_NAME, ORDINAL_POSITION";

        await using var cmd = new SqlCommand(sql, conn);
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

            var mappedType = MapSqlServerType(dataType);
            var desc = isNullable == "YES" ? "nullable" : null;

            tables[tableName].Columns.Add(new ColumnDef(columnName, mappedType, desc));
        }

        _logger.LogInformation("[SchemaDiscovery] Discovered {Count} tables", tables.Count);

        return tables.Values.ToList();
    }

    /// <summary>
    /// Maps a SQL Server data type to a simplified NaturalQuery type.
    /// </summary>
    /// <param name="sqlServerType">The SQL Server data type name.</param>
    /// <returns>A simplified type string (int, double, boolean, date, timestamp, or string).</returns>
    private static string MapSqlServerType(string sqlServerType) => sqlServerType.ToLowerInvariant() switch
    {
        "int" or "bigint" or "smallint" or "tinyint" => "int",
        "decimal" or "numeric" or "money" or "smallmoney" or "float" or "real" => "double",
        "bit" => "boolean",
        "date" => "date",
        "datetime" or "datetime2" or "datetimeoffset" or "smalldatetime" => "timestamp",
        "nvarchar" or "varchar" or "char" or "nchar" or "text" or "ntext" => "string",
        "uniqueidentifier" => "string",
        _ => "string"
    };
}

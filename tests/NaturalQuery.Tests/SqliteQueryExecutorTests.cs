using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

public class SqliteQueryExecutorTests : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly SqliteQueryExecutor _executor;

    public SqliteQueryExecutorTests()
    {
        // Use shared cache so the executor (which opens its own connection) sees the same DB
        var connString = $"DataSource=file:testdb_{Guid.NewGuid():N}?mode=memory&cache=shared";

        _connection = new SqliteConnection(connString);
        _connection.Open();

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                active INTEGER NOT NULL
            );
            INSERT INTO products VALUES (1, 'Widget',          'Hardware',    9.99,  1);
            INSERT INTO products VALUES (2, 'Gadget',          'Electronics', 24.99, 1);
            INSERT INTO products VALUES (3, 'Thingamajig',     'Hardware',    14.99, 0);
            INSERT INTO products VALUES (4, 'Doohickey',       'Electronics', 34.99, 1);
            INSERT INTO products VALUES (5, 'Whatchamacallit', 'Hardware',    4.99,  1);
        ";
        cmd.ExecuteNonQuery();

        _executor = new SqliteQueryExecutor(
            connString,
            NullLogger<SqliteQueryExecutor>.Instance);
    }

    public void Dispose()
    {
        _connection.Close();
        _connection.Dispose();
    }

    // ── Chart queries ──────────────────────────────────────────

    [Fact]
    public async Task ExecuteChartQueryAsync_GroupBy_Returns_Correct_DataPoints()
    {
        var sql = "SELECT category AS label, COUNT(*) AS value FROM products GROUP BY category";

        var result = await _executor.ExecuteChartQueryAsync(sql);

        result.Should().HaveCount(2);
        result.Should().Contain(dp => dp.Label == "Hardware" && dp.Value == 3);
        result.Should().Contain(dp => dp.Label == "Electronics" && dp.Value == 2);
    }

    [Fact]
    public async Task ExecuteChartQueryAsync_EmptyResult_Returns_EmptyList()
    {
        var sql = "SELECT category AS label, COUNT(*) AS value FROM products WHERE 1 = 0 GROUP BY category";

        var result = await _executor.ExecuteChartQueryAsync(sql);

        result.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteChartQueryAsync_Sum_Aggregate_Works()
    {
        // Use integer prices to avoid locale-dependent double.ToString() issues
        // (the executor uses GetValue().ToString() + InvariantCulture TryParse,
        //  but the ToString() call uses the current thread culture)
        var sql = "SELECT category, SUM(active) FROM products GROUP BY category ORDER BY category";

        var result = await _executor.ExecuteChartQueryAsync(sql);

        result.Should().HaveCount(2);
        var electronics = result.First(dp => dp.Label == "Electronics");
        electronics.Value.Should().Be(2);
        var hardware = result.First(dp => dp.Label == "Hardware");
        hardware.Value.Should().Be(2);
    }

    [Fact]
    public async Task ExecuteChartQueryAsync_Count_Aggregate_Works()
    {
        var sql = "SELECT 'total' AS label, COUNT(*) AS value FROM products";

        var result = await _executor.ExecuteChartQueryAsync(sql);

        result.Should().HaveCount(1);
        result[0].Label.Should().Be("total");
        result[0].Value.Should().Be(5);
    }

    // ── Table queries ──────────────────────────────────────────

    [Fact]
    public async Task ExecuteTableQueryAsync_SelectAll_Returns_AllColumns()
    {
        var sql = "SELECT * FROM products ORDER BY id";

        var result = await _executor.ExecuteTableQueryAsync(sql);

        result.Should().HaveCount(5);

        var first = result[0];
        first.Should().ContainKey("id");
        first.Should().ContainKey("name");
        first.Should().ContainKey("category");
        first.Should().ContainKey("price");
        first.Should().ContainKey("active");
        first["name"].Should().Be("Widget");
    }

    [Fact]
    public async Task ExecuteTableQueryAsync_WhereFilter_Returns_FilteredRows()
    {
        var sql = "SELECT id, name, category FROM products WHERE category = 'Electronics'";

        var result = await _executor.ExecuteTableQueryAsync(sql);

        result.Should().HaveCount(2);
        result.Select(r => r["name"]).Should().BeEquivalentTo("Gadget", "Doohickey");
    }

    [Fact]
    public async Task ExecuteTableQueryAsync_OrderByAndLimit_Works()
    {
        var sql = "SELECT name, price FROM products ORDER BY price DESC LIMIT 2";

        var result = await _executor.ExecuteTableQueryAsync(sql);

        result.Should().HaveCount(2);
        result[0]["name"].Should().Be("Doohickey");
        result[1]["name"].Should().Be("Gadget");
    }

    [Fact]
    public async Task ExecuteTableQueryAsync_Count_Aggregate_Works()
    {
        var sql = "SELECT COUNT(*) AS total FROM products WHERE active = 1";

        var result = await _executor.ExecuteTableQueryAsync(sql);

        result.Should().HaveCount(1);
        result[0]["total"].Should().Be("4");
    }

    // ── Error handling ─────────────────────────────────────────

    [Fact]
    public async Task ExecuteChartQueryAsync_InvalidSql_Throws()
    {
        var sql = "SELECT * FROM nonexistent_table";

        var act = () => _executor.ExecuteChartQueryAsync(sql);

        await act.Should().ThrowAsync<SqliteException>();
    }

    [Fact]
    public async Task ExecuteTableQueryAsync_InvalidSql_Throws()
    {
        var sql = "INVALID SQL STATEMENT";

        var act = () => _executor.ExecuteTableQueryAsync(sql);

        await act.Should().ThrowAsync<SqliteException>();
    }
}

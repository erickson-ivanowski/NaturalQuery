using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using NaturalQuery.Discovery;

namespace NaturalQuery.Tests;

public class SqliteSchemaDiscoveryTests : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly string _connString;

    public SqliteSchemaDiscoveryTests()
    {
        _connString = $"DataSource=file:schemadb_{Guid.NewGuid():N}?mode=memory&cache=shared";
        _connection = new SqliteConnection(_connString);
        _connection.Open();
    }

    public void Dispose()
    {
        _connection.Close();
        _connection.Dispose();
    }

    private SqliteSchemaDiscovery CreateDiscovery() =>
        new(_connString, NullLogger<SqliteSchemaDiscovery>.Instance);

    private void ExecuteSql(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    // ── Table discovery ────────────────────────────────────────

    [Fact]
    public async Task DiscoverAsync_WithTables_Returns_CorrectTableNames()
    {
        ExecuteSql("CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL)");
        ExecuteSql("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables.Should().HaveCount(2);
        tables.Select(t => t.Name).Should().BeEquivalentTo("customers", "orders");
    }

    [Fact]
    public async Task DiscoverAsync_EmptyDatabase_Returns_EmptyList()
    {
        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables.Should().BeEmpty();
    }

    [Fact]
    public async Task DiscoverAsync_Excludes_SqliteSystemTables()
    {
        // sqlite_master and sqlite_sequence are system tables
        ExecuteSql("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)");
        // Inserting a row causes sqlite_sequence to be created internally
        ExecuteSql("INSERT INTO users (name) VALUES ('test')");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables.Should().HaveCount(1);
        tables[0].Name.Should().Be("users");
        tables.Should().NotContain(t => t.Name.StartsWith("sqlite_"));
    }

    // ── Column discovery ───────────────────────────────────────

    [Fact]
    public async Task DiscoverAsync_Returns_CorrectColumns()
    {
        ExecuteSql(@"
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL,
                active INTEGER NOT NULL
            )");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables.Should().HaveCount(1);
        var columns = tables[0].Columns;

        columns.Should().HaveCount(4);
        columns.Select(c => c.Name).Should().BeEquivalentTo("id", "name", "price", "active");
    }

    // ── Type mapping ───────────────────────────────────────────

    [Fact]
    public async Task DiscoverAsync_Maps_INTEGER_To_Int()
    {
        ExecuteSql("CREATE TABLE t (col INTEGER)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Type.Should().Be("int");
    }

    [Fact]
    public async Task DiscoverAsync_Maps_TEXT_To_String()
    {
        ExecuteSql("CREATE TABLE t (col TEXT)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Type.Should().Be("string");
    }

    [Fact]
    public async Task DiscoverAsync_Maps_REAL_To_Double()
    {
        ExecuteSql("CREATE TABLE t (col REAL)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Type.Should().Be("double");
    }

    [Fact]
    public async Task DiscoverAsync_Maps_BLOB_To_String()
    {
        ExecuteSql("CREATE TABLE t (col BLOB)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Type.Should().Be("string");
    }

    // ── Nullable detection ─────────────────────────────────────

    [Fact]
    public async Task DiscoverAsync_NotNull_Column_Has_No_Description()
    {
        ExecuteSql("CREATE TABLE t (col INTEGER NOT NULL)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Description.Should().BeNull();
    }

    [Fact]
    public async Task DiscoverAsync_Nullable_Column_Has_Nullable_Description()
    {
        ExecuteSql("CREATE TABLE t (col TEXT)");

        var discovery = CreateDiscovery();
        var tables = await discovery.DiscoverAsync();

        tables[0].Columns[0].Description.Should().Be("nullable");
    }

    // ── SchemaFilter is ignored ────────────────────────────────

    [Fact]
    public async Task DiscoverAsync_SchemaFilter_Is_Ignored_For_Sqlite()
    {
        ExecuteSql("CREATE TABLE items (id INTEGER PRIMARY KEY)");

        var discovery = CreateDiscovery();

        // Passing a schema filter should still work (it's ignored for SQLite)
        var tables = await discovery.DiscoverAsync(schemaFilter: "public");

        tables.Should().HaveCount(1);
        tables[0].Name.Should().Be("items");
    }
}

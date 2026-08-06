using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-022: MySQL/MariaDB executor shape tests (no live server): construction,
/// transaction wrapping, row cap, and timeout options follow the same contract
/// as the existing executors.
/// </summary>
public class MySqlQueryExecutorTests
{
    private const string ConnectionString = "Server=localhost;Database=test;Uid=user;Pwd=pass;";

    [Fact]
    public void Constructor_WithValidParameters_CreatesInstance()
    {
        var executor = new MySqlQueryExecutor(
            ConnectionString,
            NullLogger<MySqlQueryExecutor>.Instance);

        executor.Should().NotBeNull();
        executor.Should().BeAssignableTo<IQueryExecutor>();
    }

    [Fact]
    public void Constructor_WithCustomTimeout_CreatesInstance()
    {
        var executor = new MySqlQueryExecutor(
            ConnectionString,
            NullLogger<MySqlQueryExecutor>.Instance,
            timeoutSeconds: 60);

        executor.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithTransactionWrapping_CreatesInstance()
    {
        var executor = new MySqlQueryExecutor(
            ConnectionString,
            NullLogger<MySqlQueryExecutor>.Instance,
            timeoutSeconds: 30,
            wrapInTransaction: true);

        executor.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithRowCap_CreatesInstance()
    {
        var executor = new MySqlQueryExecutor(
            ConnectionString,
            NullLogger<MySqlQueryExecutor>.Instance,
            timeoutSeconds: 30,
            wrapInTransaction: false,
            maxResultRows: 10_000);

        executor.Should().NotBeNull();
    }

    [Fact]
    public async Task Unreachable_Server_Should_Propagate_Connection_Failure()
    {
        var executor = new MySqlQueryExecutor(
            "Server=127.0.0.1;Port=1;Database=test;Uid=u;Pwd=p;Connection Timeout=1;",
            NullLogger<MySqlQueryExecutor>.Instance,
            timeoutSeconds: 1);

        var act = () => executor.ExecuteTableQueryAsync("SELECT 1");

        await act.Should().ThrowAsync<Exception>();
    }
}

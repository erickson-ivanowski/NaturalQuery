using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

public class SqlServerQueryExecutorTests
{
    [Fact]
    public void Constructor_WithValidParameters_CreatesInstance()
    {
        var executor = new SqlServerQueryExecutor(
            "Server=localhost;Database=test;Trusted_Connection=true;",
            NullLogger<SqlServerQueryExecutor>.Instance);

        executor.Should().NotBeNull();
        executor.Should().BeAssignableTo<IQueryExecutor>();
    }

    [Fact]
    public void Constructor_WithCustomTimeout_CreatesInstance()
    {
        var executor = new SqlServerQueryExecutor(
            "Server=localhost;Database=test;Trusted_Connection=true;",
            NullLogger<SqlServerQueryExecutor>.Instance,
            timeoutSeconds: 60);

        executor.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithTransactionWrapping_CreatesInstance()
    {
        var executor = new SqlServerQueryExecutor(
            "Server=localhost;Database=test;Trusted_Connection=true;",
            NullLogger<SqlServerQueryExecutor>.Instance,
            timeoutSeconds: 30,
            wrapInTransaction: true);

        executor.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_ImplementsIQueryExecutor()
    {
        var executor = new SqlServerQueryExecutor(
            "Server=localhost;Database=test;",
            NullLogger<SqlServerQueryExecutor>.Instance);

        executor.Should().BeAssignableTo<IQueryExecutor>();
    }
}

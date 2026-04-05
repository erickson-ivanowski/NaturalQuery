using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using NaturalQuery.Discovery;

namespace NaturalQuery.Tests;

public class SqlServerSchemaDiscoveryTests
{
    [Fact]
    public void Constructor_WithValidParameters_CreatesInstance()
    {
        var discovery = new SqlServerSchemaDiscovery(
            "Server=localhost;Database=test;Trusted_Connection=true;",
            NullLogger<SqlServerSchemaDiscovery>.Instance);

        discovery.Should().NotBeNull();
        discovery.Should().BeAssignableTo<ISchemaDiscovery>();
    }

    [Fact]
    public void Constructor_ImplementsISchemaDiscovery()
    {
        var discovery = new SqlServerSchemaDiscovery(
            "Server=localhost;Database=test;",
            NullLogger<SqlServerSchemaDiscovery>.Instance);

        discovery.Should().BeAssignableTo<ISchemaDiscovery>();
    }
}

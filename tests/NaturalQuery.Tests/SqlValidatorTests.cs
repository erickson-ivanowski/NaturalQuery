using FluentAssertions;
using NaturalQuery.Validation;

namespace NaturalQuery.Tests;

public class SqlValidatorTests
{
    [Fact]
    public void Select_Query_Should_Be_Valid()
    {
        var result = SqlValidator.Validate("SELECT * FROM users");
        result.Should().BeNull();
    }

    [Fact]
    public void With_CTE_Query_Should_Be_Valid()
    {
        var result = SqlValidator.Validate("WITH latest AS (SELECT * FROM users) SELECT * FROM latest");
        result.Should().BeNull();
    }

    [Fact]
    public void Delete_Query_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("DELETE FROM users WHERE id = 1");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Drop_Table_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("DROP TABLE users");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Update_Query_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("UPDATE users SET name = 'x'");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Insert_In_String_Literal_Should_Not_Be_Rejected()
    {
        var result = SqlValidator.Validate("SELECT * FROM events WHERE type IN ('INSERT', 'MODIFY')");
        result.Should().BeNull();
    }

    [Fact]
    public void Empty_Query_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Non_Select_Query_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("EXPLAIN SELECT * FROM users");
        result.Should().Contain("Only SELECT");
    }

    [Fact]
    public void Tenant_Filter_Missing_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate(
            "SELECT * FROM users",
            tenantIdColumn: "tenant_id",
            tenantId: "abc-123");

        result.Should().Contain("tenant");
    }

    [Fact]
    public void Tenant_Filter_Present_Should_Be_Valid()
    {
        var result = SqlValidator.Validate(
            "SELECT * FROM users WHERE tenant_id = 'abc-123'",
            tenantIdColumn: "tenant_id",
            tenantId: "abc-123");

        result.Should().BeNull();
    }

    [Fact]
    public void Additional_Forbidden_Keywords_Should_Be_Checked()
    {
        var result = SqlValidator.Validate(
            "SELECT * FROM users UNION SELECT * FROM admins",
            additionalForbidden: new[] { "UNION " });

        result.Should().Contain("Forbidden");
    }

    [Fact]
    public void Truncate_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("TRUNCATE TABLE users");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Create_Table_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("CREATE TABLE evil (id INT)");
        result.Should().NotBeNull();
    }

    [Fact]
    public void Multiple_Statements_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("SELECT 1; SELECT 2");
        result.Should().Contain("Multiple SQL statements");
    }

    [Fact]
    public void Trailing_Semicolon_Should_Be_Allowed()
    {
        var result = SqlValidator.Validate("SELECT * FROM users;");
        result.Should().BeNull();
    }

    [Fact]
    public void Semicolon_Inside_String_Literal_Should_Be_Allowed()
    {
        var result = SqlValidator.Validate("SELECT * FROM users WHERE name = 'foo;bar'");
        result.Should().BeNull();
    }

    [Fact]
    public void Multiple_Statements_With_Injection_Should_Be_Rejected()
    {
        var result = SqlValidator.Validate("SELECT * FROM users; DELETE FROM users");
        result.Should().NotBeNull();
    }
}

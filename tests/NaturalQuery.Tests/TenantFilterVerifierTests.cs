using FluentAssertions;
using NaturalQuery.Validation;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-007: the tenant value must be applied as a real filter on the configured
/// tenant column — mere presence anywhere in the query text is not enough.
/// </summary>
public class TenantFilterVerifierTests
{
    private const string Column = "tenant_id";
    private const string Tenant = "abc-123";

    // --- Real filters (accepted) ---

    [Theory]
    [InlineData("SELECT * FROM users WHERE tenant_id = 'abc-123'")]
    [InlineData("SELECT * FROM users WHERE TENANT_ID = 'abc-123'")]
    [InlineData("SELECT * FROM users WHERE tenant_id='abc-123'")]
    [InlineData("SELECT * FROM users WHERE tenant_id   =   'abc-123'")]
    [InlineData("SELECT * FROM users WHERE tenant_id\n= 'abc-123'")]
    [InlineData("SELECT * FROM users u WHERE u.tenant_id = 'abc-123'")]
    [InlineData("SELECT * FROM orders o JOIN users u ON o.uid = u.id WHERE u.tenant_id = 'abc-123'")]
    [InlineData("WITH t AS (SELECT * FROM users WHERE tenant_id = 'abc-123') SELECT * FROM t")]
    [InlineData("SELECT * FROM users WHERE tenant_id = 'abc-123' AND age > 21")]
    public void Real_Tenant_Filter_Should_Be_Accepted(string sql)
    {
        TenantFilterVerifier.HasTenantFilter(sql, Column, Tenant).Should().BeTrue();
    }

    // --- Presence without a real filter (rejected) ---

    [Theory]
    [InlineData("SELECT * FROM users WHERE name = 'abc-123'")]                    // value on wrong column
    [InlineData("SELECT * FROM users -- tenant_id = 'abc-123'")]                  // filter only in comment
    [InlineData("SELECT * FROM users /* tenant_id = 'abc-123' */")]               // filter only in block comment
    [InlineData("SELECT 'abc-123' AS tenant FROM users")]                         // value only as literal
    [InlineData("SELECT * FROM users WHERE note = 'tenant_id = abc-123'")]        // whole filter inside a literal
    [InlineData("SELECT * FROM users")]                                           // no filter at all
    [InlineData("SELECT * FROM users WHERE tenant_id = 'other-tenant'")]          // filter on wrong value
    [InlineData("SELECT * FROM users WHERE tenant_id != 'abc-123'")]              // wrong operator
    [InlineData("SELECT * FROM users WHERE tenant_id LIKE 'abc-123%'")]           // not an equality filter
    public void Missing_Or_Fake_Tenant_Filter_Should_Be_Rejected(string sql)
    {
        TenantFilterVerifier.HasTenantFilter(sql, Column, Tenant).Should().BeFalse();
    }

    // --- Robustness ---

    [Fact]
    public void Column_Name_Should_Not_Match_As_Substring_Of_Another_Column()
    {
        // "other_tenant_id" must not satisfy a "tenant_id" filter requirement.
        TenantFilterVerifier.HasTenantFilter(
            "SELECT * FROM users WHERE other_tenant_id = 'abc-123'", Column, Tenant)
            .Should().BeFalse();
    }

    [Fact]
    public void Alias_Qualified_Column_With_Whitespace_Should_Be_Accepted()
    {
        TenantFilterVerifier.HasTenantFilter(
            "SELECT * FROM users u WHERE u . tenant_id = 'abc-123'", Column, Tenant)
            .Should().BeTrue();
    }

    [Fact]
    public void Comment_Between_Column_And_Value_Should_Still_Verify()
    {
        TenantFilterVerifier.HasTenantFilter(
            "SELECT * FROM users WHERE tenant_id /*x*/ = 'abc-123'", Column, Tenant)
            .Should().BeTrue();
    }
}

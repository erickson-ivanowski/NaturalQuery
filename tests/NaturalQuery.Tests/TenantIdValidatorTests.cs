using FluentAssertions;
using NaturalQuery.Validation;

namespace NaturalQuery.Tests;

/// <summary>
/// SC-002 corpus: tenant identifiers are untrusted input — injection syntax must be
/// rejected before any query is built; well-formed identifiers must keep working.
/// </summary>
public class TenantIdValidatorTests
{
    // --- Injection corpus (must be rejected) ---

    [Theory]
    [InlineData("abc' OR '1'='1")]
    [InlineData("abc'--")]
    [InlineData("abc/*comment*/")]
    [InlineData("abc--comment")]
    [InlineData("abc;DROP TABLE users")]
    [InlineData("abc def")]
    [InlineData("abc\tdef")]
    [InlineData("abc\ndef")]
    [InlineData("abc\"def")]
    [InlineData("abc'def")]
    [InlineData("abc(def)")]
    [InlineData("abc=def")]
    [InlineData("abc%def")]
    public void Injection_Syntax_Should_Be_Rejected(string tenantId)
    {
        TenantIdValidator.Validate(tenantId).Should().NotBeNull();
    }

    [Fact]
    public void Over_128_Chars_Should_Be_Rejected()
    {
        var tooLong = new string('a', 129);
        TenantIdValidator.Validate(tooLong).Should().NotBeNull();
    }

    // --- Valid corpus (must be accepted) ---

    [Theory]
    [InlineData("abc")]
    [InlineData("abc-123")]
    [InlineData("abc_123")]
    [InlineData("tenant.prod.eu")]
    [InlineData("ABC-def_123.x")]
    [InlineData("a")]
    public void Well_Formed_Identifier_Should_Be_Accepted(string tenantId)
    {
        TenantIdValidator.Validate(tenantId).Should().BeNull();
    }

    [Fact]
    public void Exactly_128_Chars_Should_Be_Accepted()
    {
        var max = new string('a', 128);
        TenantIdValidator.Validate(max).Should().BeNull();
    }

    // --- Not-provided semantics ---

    [Fact]
    public void Null_Should_Be_Treated_As_Not_Provided()
    {
        TenantIdValidator.Validate(null).Should().BeNull();
    }

    [Fact]
    public void Empty_String_Should_Be_Treated_As_Not_Provided()
    {
        TenantIdValidator.Validate("").Should().BeNull();
    }

    // --- Custom policy override ---

    [Fact]
    public void Custom_Pattern_Should_Override_Default()
    {
        // Digits-only policy
        TenantIdValidator.Validate("abc", @"^\d{1,10}$").Should().NotBeNull();
        TenantIdValidator.Validate("12345", @"^\d{1,10}$").Should().BeNull();
    }

    [Fact]
    public void Custom_Pattern_Must_Match_Entire_Input()
    {
        // Even a permissive custom pattern must not allow partial-match bypass.
        TenantIdValidator.Validate("12'--", @"\d+").Should().NotBeNull();
    }
}

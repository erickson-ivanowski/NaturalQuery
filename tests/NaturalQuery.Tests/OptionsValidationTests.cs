using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NaturalQuery.Extensions;

namespace NaturalQuery.Tests;

/// <summary>
/// SC-010: invalid configurations fail at startup with a message naming the exact
/// problem; every currently-valid configuration continues to start unchanged.
/// </summary>
public class OptionsValidationTests
{
    private static IOptions<NaturalQueryOptions> Validate(Action<NaturalQueryOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNaturalQuery(configure);

        using var sp = services.BuildServiceProvider();
        return sp.GetRequiredService<IOptions<NaturalQueryOptions>>();
    }

    private static Action Act(Action<NaturalQueryOptions> configure) => () => Validate(configure).Value.GetHashCode();

    // --- Invalid configurations must fail with a named problem ---

    [Fact]
    public void TenantColumn_Without_Placeholder_Should_Fail()
    {
        var act = Act(o =>
        {
            o.TenantIdColumn = "tenant_id";
            o.TenantIdPlaceholder = null;
        });

        act.Should().Throw<OptionsValidationException>().WithMessage("*TenantIdPlaceholder*");
    }

    [Fact]
    public void TenantPlaceholder_Without_Column_Should_Fail()
    {
        var act = Act(o =>
        {
            o.TenantIdColumn = null;
            o.TenantIdPlaceholder = "{TENANT_ID}";
        });

        act.Should().Throw<OptionsValidationException>().WithMessage("*TenantIdColumn*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void MaxQuestionLength_NonPositive_Should_Fail(int value)
    {
        var act = Act(o => o.MaxQuestionLength = value);

        act.Should().Throw<OptionsValidationException>().WithMessage("*MaxQuestionLength*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void MaxResultRows_NonPositive_Should_Fail(int value)
    {
        var act = Act(o => o.MaxResultRows = value);

        act.Should().Throw<OptionsValidationException>().WithMessage("*MaxResultRows*");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void QueryTimeoutSeconds_NonPositive_Should_Fail(int value)
    {
        var act = Act(o => o.QueryTimeoutSeconds = value);

        act.Should().Throw<OptionsValidationException>().WithMessage("*QueryTimeoutSeconds*");
    }

    [Theory]
    [InlineData(0.4)]
    [InlineData(1.1)]
    public void SemanticCacheSimilarityThreshold_Outside_Range_Should_Fail(double value)
    {
        var act = Act(o => o.SemanticCacheSimilarityThreshold = value);

        act.Should().Throw<OptionsValidationException>().WithMessage("*SemanticCacheSimilarityThreshold*");
    }

    [Fact]
    public void TenantIdPattern_Invalid_Regex_Should_Fail()
    {
        var act = Act(o => o.TenantIdPattern = "(unterminated");

        act.Should().Throw<OptionsValidationException>();
    }

    // --- Currently-valid configurations must keep starting ---

    [Fact]
    public void Default_Configuration_Should_Be_Valid()
    {
        var act = Act(_ => { });

        act.Should().NotThrow();
    }

    [Fact]
    public void SingleTenant_Configuration_Should_Be_Valid()
    {
        var act = Act(o =>
        {
            o.TenantIdColumn = null;
            o.TenantIdPlaceholder = null;
        });

        act.Should().NotThrow();
    }

    [Fact]
    public void MultiTenant_Configuration_Should_Be_Valid()
    {
        var act = Act(o =>
        {
            o.TenantIdColumn = "tenant_id";
            o.TenantIdPlaceholder = "{TENANT_ID}";
        });

        act.Should().NotThrow();
    }

    [Fact]
    public void Boundary_SimilarityThreshold_Should_Be_Valid()
    {
        var act = Act(o => o.SemanticCacheSimilarityThreshold = 1.0);

        act.Should().NotThrow();
    }
}

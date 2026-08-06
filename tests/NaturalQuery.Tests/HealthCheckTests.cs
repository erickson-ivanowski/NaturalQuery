using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using NaturalQuery.Extensions;
using NaturalQuery.Health;
using NaturalQuery.Models;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-026: health checks reporting AI provider and database reachability.
/// </summary>
public class HealthCheckTests
{
    private readonly Mock<IQueryExecutor> _executorMock = new();
    private readonly Mock<ILlmProvider> _llmMock = new();

    private NaturalQueryHealthCheck CreateHealthCheck() =>
        new(_executorMock.Object, _llmMock.Object);

    [Fact]
    public async Task Should_Report_Healthy_When_Executor_Reachable()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<Dictionary<string, string>>());
        var check = CreateHealthCheck();

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Should_Report_Unhealthy_When_Executor_Unreachable()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("connection refused"));
        var check = CreateHealthCheck();

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Description.Should().Contain("database", "unhealthy report should name what's unreachable");
    }

    [Fact]
    public async Task Should_Recover_After_Executor_Becomes_Reachable_Again()
    {
        var callCount = 0;
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount == 1) throw new InvalidOperationException("down");
                return Task.FromResult(new List<Dictionary<string, string>>());
            });
        var check = CreateHealthCheck();

        var first = await check.CheckHealthAsync(new HealthCheckContext());
        var second = await check.CheckHealthAsync(new HealthCheckContext());

        first.Status.Should().Be(HealthStatus.Unhealthy);
        second.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task Should_Not_Make_A_Billable_Llm_Call()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<Dictionary<string, string>>());
        var check = CreateHealthCheck();

        await check.CheckHealthAsync(new HealthCheckContext());

        _llmMock.Verify(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public void Registration_Extension_Should_Add_A_Named_Health_Check()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        services.AddSingleton(_executorMock.Object);
        services.AddSingleton(_llmMock.Object);
        var builder = services.AddHealthChecks();

        var act = () => builder.AddNaturalQueryHealthCheck();

        act.Should().NotThrow();
    }
}

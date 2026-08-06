using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Models;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-032: preview (dry-run) mode returns the generated query and estimated cost
/// without executing; approved execution re-applies all safety validation.
/// </summary>
public class PreviewExecuteTests
{
    private const string SafeLlmJson = "{\"sql\":\"SELECT COUNT(*) AS value FROM users\",\"chartType\":\"metric\",\"title\":\"t\",\"description\":\"d\"}";

    private readonly Mock<ILlmProvider> _llmMock = new();
    private readonly Mock<IQueryExecutor> _executorMock = new();

    private NaturalQueryEngine CreateEngine(Action<NaturalQueryOptions>? configure = null)
    {
        var options = new NaturalQueryOptions();
        configure?.Invoke(options);
        _llmMock
            .Setup(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LlmResponse(SafeLlmJson, 10));
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<DataPoint> { new("total", 3) });

        return new NaturalQueryEngine(
            _llmMock.Object, _executorMock.Object, Options.Create(options),
            NullLogger<NaturalQueryEngine>.Instance);
    }

    [Fact]
    public async Task Preview_Should_Return_Sql_Without_Executing()
    {
        var engine = CreateEngine();

        var preview = await engine.PreviewAsync("how many users?");

        preview.Sql.Should().Contain("SELECT");
        _executorMock.Verify(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        _executorMock.Verify(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Preview_Should_Be_Rejected_When_Sql_Is_Dangerous()
    {
        var engine = CreateEngine();
        _llmMock
            .Setup(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LlmResponse("{\"sql\":\"SELECT 1; DROP TABLE users\",\"chartType\":\"table\"}", 5));

        var act = () => engine.PreviewAsync("evil");

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task ExecuteApproved_Should_Run_The_Query()
    {
        var engine = CreateEngine();
        var preview = await engine.PreviewAsync("how many users?");

        var result = await engine.ExecuteApprovedAsync(preview.Sql);

        result.ChartData.Should().NotBeNull();
        _executorMock.Verify(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteApproved_Should_ReValidate_Dangerous_Sql()
    {
        var engine = CreateEngine();

        var act = () => engine.ExecuteApprovedAsync("SELECT 1; DELETE FROM users");

        await act.Should().ThrowAsync<InvalidOperationException>();
        _executorMock.Verify(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteApproved_Should_ReValidate_Tenant_Filter()
    {
        var engine = CreateEngine(o =>
        {
            o.TenantIdColumn = "tenant_id";
        });

        // A stale approval whose SQL doesn't actually filter on the tenant column
        // must be rejected at execution time (config/tenant may have changed since preview).
        var act = () => engine.ExecuteApprovedAsync("SELECT COUNT(*) AS value FROM users", tenantId: "abc-123");

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task ExecuteApproved_Should_Apply_Masking_And_Caps_Like_Normal_Execution()
    {
        var engine = CreateEngine(o => o.MaxResultRows = 10);
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Enumerable.Range(0, 20).Select(i => new DataPoint($"l{i}", i)).ToList());

        var result = await engine.ExecuteApprovedAsync("SELECT status AS label, COUNT(*) AS value FROM users GROUP BY status");

        result.ChartData.Should().HaveCount(10);
        result.Truncated.Should().BeTrue();
    }
}

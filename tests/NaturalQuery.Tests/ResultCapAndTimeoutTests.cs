using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Models;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-016 / FR-017: results exceeding MaxResultRows are truncated and marked;
/// execution past QueryTimeoutSeconds is cancelled and reported as a timeout.
/// </summary>
public class ResultCapAndTimeoutTests
{
    private const string TableLlmJson = "{\"sql\":\"SELECT name FROM users\",\"chartType\":\"table\",\"title\":\"t\",\"description\":\"d\"}";
    private const string ChartLlmJson = "{\"sql\":\"SELECT status AS label, COUNT(*) AS value FROM users GROUP BY status\",\"chartType\":\"bar\",\"title\":\"t\",\"description\":\"d\"}";

    private readonly Mock<ILlmProvider> _llmMock = new();
    private readonly Mock<IQueryExecutor> _executorMock = new();

    private NaturalQueryEngine CreateEngine(Action<NaturalQueryOptions>? configure = null, string llmJson = TableLlmJson)
    {
        var options = new NaturalQueryOptions();
        configure?.Invoke(options);

        _llmMock
            .Setup(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LlmResponse(llmJson, 10));

        return new NaturalQueryEngine(
            _llmMock.Object,
            _executorMock.Object,
            Options.Create(options),
            NullLogger<NaturalQueryEngine>.Instance);
    }

    private static List<Dictionary<string, string>> Rows(int count) =>
        Enumerable.Range(0, count)
            .Select(i => new Dictionary<string, string> { ["name"] = $"row-{i}" })
            .ToList();

    // --- Row cap on table data (FR-016) ---

    [Fact]
    public async Task Table_Result_Over_Cap_Should_Be_Truncated_And_Marked()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Rows(15));
        var engine = CreateEngine(o => o.MaxResultRows = 10);

        var result = await engine.AskAsync("all users");

        result.TableData.Should().HaveCount(10);
        result.Truncated.Should().BeTrue();
    }

    [Fact]
    public async Task Table_Result_Under_Cap_Should_Not_Be_Marked()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Rows(5));
        var engine = CreateEngine(o => o.MaxResultRows = 10);

        var result = await engine.AskAsync("all users");

        result.TableData.Should().HaveCount(5);
        result.Truncated.Should().BeFalse();
    }

    [Fact]
    public async Task Result_Exactly_At_Cap_Should_Not_Be_Marked()
    {
        // The query's own LIMIT was the binding constraint — no truncation marker.
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Rows(10));
        var engine = CreateEngine(o => o.MaxResultRows = 10);

        var result = await engine.AskAsync("top 10 users");

        result.TableData.Should().HaveCount(10);
        result.Truncated.Should().BeFalse();
    }

    // --- Row cap on chart data ---

    [Fact]
    public async Task Chart_Result_Over_Cap_Should_Be_Truncated_And_Marked()
    {
        _executorMock
            .Setup(e => e.ExecuteChartQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Enumerable.Range(0, 15).Select(i => new DataPoint($"l{i}", i)).ToList());
        var engine = CreateEngine(o => o.MaxResultRows = 10, llmJson: ChartLlmJson);

        var result = await engine.AskAsync("users by status");

        result.ChartData.Should().HaveCount(10);
        result.Truncated.Should().BeTrue();
    }

    // --- Execution timeout (FR-017) ---

    [Fact]
    public async Task Execution_Past_Timeout_Should_Be_Cancelled_And_Reported_As_Timeout()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, CancellationToken token) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(30), token);
                return Rows(1);
            });
        var engine = CreateEngine(o => o.QueryTimeoutSeconds = 1);

        var act = () => engine.AskAsync("slow question");

        await act.Should().ThrowAsync<TimeoutException>();
    }

    [Fact]
    public async Task Fast_Execution_Should_Not_Be_Affected_By_Timeout()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Rows(1));
        var engine = CreateEngine(o => o.QueryTimeoutSeconds = 1);

        var result = await engine.AskAsync("fast question");

        result.TableData.Should().HaveCount(1);
    }

    [Fact]
    public async Task Caller_Cancellation_Should_Not_Be_Reported_As_Timeout()
    {
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, CancellationToken token) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(30), token);
                return Rows(1);
            });
        var engine = CreateEngine(o => o.QueryTimeoutSeconds = 60);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var act = () => engine.AskAsync("slow question", ct: cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }
}

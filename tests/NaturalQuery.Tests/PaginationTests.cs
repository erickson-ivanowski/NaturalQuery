using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Models;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-031: paging through large result sets with stable ordering and without
/// additional AI calls per page.
/// </summary>
public class PaginationTests
{
    private const string TableLlmJson = "{\"sql\":\"SELECT name FROM users\",\"chartType\":\"table\",\"title\":\"t\",\"description\":\"d\"}";

    private readonly Mock<ILlmProvider> _llmMock = new();
    private readonly Mock<IQueryExecutor> _executorMock = new();

    private NaturalQueryEngine CreateEngine(int rowCount)
    {
        var options = new NaturalQueryOptions();
        _llmMock
            .Setup(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new LlmResponse(TableLlmJson, 10));
        _executorMock
            .Setup(e => e.ExecuteTableQueryAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Enumerable.Range(0, rowCount)
                .Select(i => new Dictionary<string, string> { ["name"] = $"row-{i:D3}" })
                .ToList());

        return new NaturalQueryEngine(
            _llmMock.Object, _executorMock.Object, Options.Create(options),
            NullLogger<NaturalQueryEngine>.Instance);
    }

    [Fact]
    public async Task First_Page_Should_Trigger_One_Ai_Call()
    {
        var engine = CreateEngine(25);

        await engine.AskPagedAsync("list users", page: 1, pageSize: 10);

        _llmMock.Verify(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Second_Page_Should_Not_Trigger_Additional_Ai_Call()
    {
        var engine = CreateEngine(25);

        await engine.AskPagedAsync("list users", page: 1, pageSize: 10);
        await engine.AskPagedAsync("list users", page: 2, pageSize: 10);

        _llmMock.Verify(p => p.GenerateAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Pages_Should_Slice_With_Stable_Ordering()
    {
        var engine = CreateEngine(25);

        var page1 = await engine.AskPagedAsync("list users", page: 1, pageSize: 10);
        var page2 = await engine.AskPagedAsync("list users", page: 2, pageSize: 10);
        var page3 = await engine.AskPagedAsync("list users", page: 3, pageSize: 10);

        page1.TableData.Should().HaveCount(10);
        page2.TableData.Should().HaveCount(10);
        page3.TableData.Should().HaveCount(5);

        page1.TableData![0]["name"].Should().Be("row-000");
        page2.TableData![0]["name"].Should().Be("row-010");
        page3.TableData![0]["name"].Should().Be("row-020");
    }

    [Fact]
    public async Task Truncated_Flag_Should_Be_Inherited_Across_Pages()
    {
        var engine = CreateEngine(20_000);

        var page1 = await engine.AskPagedAsync("list users", page: 1, pageSize: 10);

        page1.Truncated.Should().BeTrue(); // MaxResultRows default (10,000) binds
    }

    [Fact]
    public async Task Out_Of_Range_Page_Should_Return_Empty_Data()
    {
        var engine = CreateEngine(5);

        var page = await engine.AskPagedAsync("list users", page: 5, pageSize: 10);

        page.TableData.Should().BeEmpty();
    }
}

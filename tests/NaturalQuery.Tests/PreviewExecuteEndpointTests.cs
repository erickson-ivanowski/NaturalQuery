using System.Net;
using System.Net.Http.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using NaturalQuery.Extensions;
using NaturalQuery.Models;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-032 / T068: POST {prefix}/preview and POST {prefix}/execute routes, plus
/// page/pageSize pagination parameters on GET and POST {prefix}.
/// </summary>
public class PreviewExecuteEndpointTests : IAsyncLifetime
{
    private readonly Mock<INaturalQueryEngine> _engineMock = new();
    private WebApplication? _app;
    private HttpClient? _client;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app != null) await _app.DisposeAsync();
    }

    private async Task<HttpClient> StartAppAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_engineMock.Object);
        builder.Services.Configure<NaturalQueryOptions>(_ => { });

        _app = builder.Build();
        _app.MapNaturalQuery("/ask");

        await _app.StartAsync();
        _client = _app.GetTestClient();
        return _client;
    }

    [Fact]
    public async Task Preview_Should_Return_Preview_Without_Executing()
    {
        _engineMock
            .Setup(e => e.PreviewAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryPreview { Sql = "SELECT 1", Title = "t" });
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask/preview", new { question = "how many users?" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("SELECT 1");
        _engineMock.Verify(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Preview_Missing_Question_Should_Be_Rejected()
    {
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask/preview", new { question = "" });

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Execute_Should_Run_The_Approved_Query()
    {
        _engineMock
            .Setup(e => e.ExecuteApprovedAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1", TableData = new List<Dictionary<string, string>>() });
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask/execute", new { sql = "SELECT 1" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        _engineMock.Verify(e => e.ExecuteApprovedAsync("SELECT 1", null, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Execute_Missing_Sql_Should_Be_Rejected()
    {
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask/execute", new { sql = "" });

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Execute_Rejected_Sql_Should_Surface_Safe_Error()
    {
        _engineMock
            .Setup(e => e.ExecuteApprovedAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Invalid query: Forbidden SQL keyword detected: DELETE"));
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask/execute", new { sql = "DELETE FROM users" });

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Get_With_Page_Should_Call_AskPagedAsync()
    {
        _engineMock
            .Setup(e => e.AskPagedAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var client = await StartAppAsync();

        var response = await client.GetAsync("/ask?q=list+users&page=2&pageSize=20");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        _engineMock.Verify(e => e.AskPagedAsync("list users", 2, 20, null, It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()), Times.Once);
        _engineMock.Verify(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Get_Without_Page_Should_Call_AskAsync()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var client = await StartAppAsync();

        var response = await client.GetAsync("/ask?q=list+users");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        _engineMock.Verify(e => e.AskPagedAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Post_With_Page_Should_Call_AskPagedAsync()
    {
        _engineMock
            .Setup(e => e.AskPagedAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var client = await StartAppAsync();

        var response = await client.PostAsJsonAsync("/ask", new { question = "list users", page = 1, pageSize = 50 });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        _engineMock.Verify(e => e.AskPagedAsync("list users", 1, 50, null, It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}

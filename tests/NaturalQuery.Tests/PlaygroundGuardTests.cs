using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NaturalQuery.Playground;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-012: the playground must refuse to serve outside development environments
/// unless the operator explicitly opts in.
/// </summary>
public class PlaygroundGuardTests : IAsyncLifetime
{
    private WebApplication? _app;
    private HttpClient? _client;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_app != null) await _app.DisposeAsync();
    }

    private async Task<HttpClient> StartAppAsync(string environment, bool? allowInProduction = null)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = environment
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        _app = builder.Build();

        if (allowInProduction.HasValue)
            _app.MapNaturalQueryPlayground("/nq-playground", "/ask", allowInProduction.Value);
        else
            _app.MapNaturalQueryPlayground();

        await _app.StartAsync();
        _client = _app.GetTestClient();
        return _client;
    }

    [Fact]
    public async Task Development_Should_Serve_Playground()
    {
        var client = await StartAppAsync("Development");

        var response = await client.GetAsync("/nq-playground");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        (await response.Content.ReadAsStringAsync()).Should().Contain("<html");
    }

    [Fact]
    public async Task Production_Without_OptIn_Should_Refuse()
    {
        var client = await StartAppAsync("Production");

        var response = await client.GetAsync("/nq-playground");

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Staging_Without_OptIn_Should_Refuse()
    {
        var client = await StartAppAsync("Staging");

        var response = await client.GetAsync("/nq-playground");

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Production_With_OptIn_Should_Serve()
    {
        var client = await StartAppAsync("Production", allowInProduction: true);

        var response = await client.GetAsync("/nq-playground");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        (await response.Content.ReadAsStringAsync()).Should().Contain("<html");
    }

    [Fact]
    public async Task Existing_Two_Argument_Call_Should_Still_Compile_And_Serve_In_Development()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        _app = builder.Build();

        // Existing public call shape (no new arguments) must keep compiling — FR-020
        _app.MapNaturalQueryPlayground("/pg", "/api/ask");

        await _app.StartAsync();
        _client = _app.GetTestClient();
        var response = await _client.GetAsync("/pg");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}

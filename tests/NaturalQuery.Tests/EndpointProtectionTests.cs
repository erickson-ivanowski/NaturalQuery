using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Encodings.Web;
using FluentAssertions;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Extensions;
using NaturalQuery.Models;

namespace NaturalQuery.Tests;

/// <summary>
/// SC-005: size limits enforced before any AI call, no internal detail in failure
/// responses, authorization attachable in one configuration step, and unchanged
/// default behavior for existing integrations.
/// </summary>
public class EndpointProtectionTests : IAsyncLifetime
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

    private async Task<HttpClient> StartAppAsync(
        NaturalQueryEndpointOptions? options = null,
        bool withAuth = false)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_engineMock.Object);
        builder.Services.Configure<NaturalQueryOptions>(_ => { });

        if (withAuth)
        {
            builder.Services.AddAuthentication("Test")
                .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>("Test", _ => { });
            builder.Services.AddAuthorization();
        }

        _app = builder.Build();

        if (withAuth)
        {
            _app.UseAuthentication();
            _app.UseAuthorization();
        }

        if (options == null)
            _app.MapNaturalQuery("/ask");
        else
            _app.MapNaturalQuery("/ask", options);

        await _app.StartAsync();
        _client = _app.GetTestClient();
        return _client;
    }

    // --- Size limits (FR-009) ---

    [Fact]
    public async Task Oversized_Question_Should_Be_Rejected_Before_Engine()
    {
        var client = await StartAppAsync(new NaturalQueryEndpointOptions { MaxQuestionLength = 100 });

        var response = await client.PostAsJsonAsync("/ask", new { question = new string('x', 101) });

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
        _engineMock.Verify(e => e.AskAsync(
            It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Oversized_Question_On_Get_Should_Be_Rejected()
    {
        var client = await StartAppAsync(new NaturalQueryEndpointOptions { MaxQuestionLength = 100 });

        var response = await client.GetAsync("/ask?q=" + new string('x', 101));

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Oversized_History_Should_Be_Rejected_Before_Engine()
    {
        var client = await StartAppAsync(new NaturalQueryEndpointOptions { MaxContextTurns = 2 });

        var context = Enumerable.Range(0, 3)
            .Select(i => new { question = $"q{i}", sql = $"SELECT {i}" })
            .ToArray();
        var response = await client.PostAsJsonAsync("/ask", new { question = "ok", context });

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
        _engineMock.Verify(e => e.AskAsync(
            It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Question_Within_Limit_Should_Reach_Engine()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var client = await StartAppAsync(new NaturalQueryEndpointOptions { MaxQuestionLength = 100 });

        var response = await client.PostAsJsonAsync("/ask", new { question = "how many users?" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // --- Safe errors (FR-010) ---

    [Fact]
    public async Task Backend_Failure_Should_Return_Safe_Error_With_CorrelationId()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Npgsql connection failed: host=secret-db.internal password=hunter2"));
        var client = await StartAppAsync(new NaturalQueryEndpointOptions());

        var response = await client.PostAsJsonAsync("/ask", new { question = "how many users?" });
        var body = await response.Content.ReadAsStringAsync();

        ((int)response.StatusCode).Should().BeGreaterThanOrEqualTo(400);
        body.Should().Contain("correlationId");
        body.Should().NotContain("Npgsql");
        body.Should().NotContain("secret-db.internal");
        body.Should().NotContain("hunter2");
    }

    [Fact]
    public async Task Validation_Failure_Should_Return_Safe_Error_With_CorrelationId()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Forbidden SQL keyword detected: DELETE"));
        var client = await StartAppAsync(new NaturalQueryEndpointOptions());

        var response = await client.PostAsJsonAsync("/ask", new { question = "delete everything" });
        var body = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        body.Should().Contain("correlationId");
    }

    // --- Authorization opt-in (FR-011) ---

    [Fact]
    public async Task RequireAuthorization_Should_Refuse_Unauthenticated()
    {
        var client = await StartAppAsync(
            new NaturalQueryEndpointOptions { RequireAuthorization = true },
            withAuth: true);

        var response = await client.PostAsJsonAsync("/ask", new { question = "hello" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task RequireAuthorization_Should_Allow_Authenticated()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1" });
        var client = await StartAppAsync(
            new NaturalQueryEndpointOptions { RequireAuthorization = true },
            withAuth: true);
        client.DefaultRequestHeaders.Add("X-Test-Auth", "yes");

        var response = await client.PostAsJsonAsync("/ask", new { question = "hello" });

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    // --- Default behavior preserved (FR-020 / no-break) ---

    [Fact]
    public async Task Default_Mapping_Should_Work_Without_New_Options()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueryResult { Sql = "SELECT 1", Title = "ok" });
        var client = await StartAppAsync();

        var response = await client.GetAsync("/ask?q=how+many+users");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        (await response.Content.ReadAsStringAsync()).Should().Contain("SELECT 1");
    }

    [Fact]
    public async Task Default_Mapping_Should_Apply_Generous_Default_Limit()
    {
        var client = await StartAppAsync();

        // Above the 2000-char default cap
        var response = await client.PostAsJsonAsync("/ask", new { question = new string('x', 2001) });

        response.StatusCode.Should().Be(HttpStatusCode.RequestEntityTooLarge);
    }

    [Fact]
    public async Task Default_Mapping_Error_Shape_Should_Be_Unchanged()
    {
        _engineMock
            .Setup(e => e.AskAsync(It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<ConversationContext?>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Rate limit exceeded. Try again later."));
        var client = await StartAppAsync();

        var response = await client.GetAsync("/ask?q=hello");
        var body = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        body.Should().Contain("Rate limit exceeded");
    }
}

/// <summary>Header-driven test authentication scheme: X-Test-Auth: yes → authenticated.</summary>
public class TestAuthHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    public TestAuthHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : base(options, logger, encoder)
    {
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!Request.Headers.ContainsKey("X-Test-Auth"))
            return Task.FromResult(AuthenticateResult.Fail("No test auth header"));

        var identity = new ClaimsIdentity(new[] { new Claim(ClaimTypes.Name, "test-user") }, "Test");
        var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), "Test");
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}

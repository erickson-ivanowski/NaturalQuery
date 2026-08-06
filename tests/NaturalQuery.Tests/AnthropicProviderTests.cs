using System.Net;
using System.Text;
using FluentAssertions;
using NaturalQuery.Providers;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-023: direct Anthropic provider with the same response contract, token
/// reporting, and error classification as existing providers.
/// </summary>
public class AnthropicProviderTests
{
    private class MockHttpHandler : HttpMessageHandler
    {
        private readonly string _responseBody;
        private readonly HttpStatusCode _statusCode;
        private readonly Exception? _exception;
        public HttpRequestMessage? LastRequest { get; private set; }
        public string? LastRequestBody { get; private set; }

        public MockHttpHandler(string responseBody, HttpStatusCode statusCode)
        {
            _responseBody = responseBody;
            _statusCode = statusCode;
        }

        public MockHttpHandler(Exception exception)
        {
            _responseBody = "";
            _statusCode = HttpStatusCode.OK;
            _exception = exception;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            if (request.Content != null)
                LastRequestBody = await request.Content.ReadAsStringAsync(cancellationToken);

            if (_exception != null)
                throw _exception;

            return new HttpResponseMessage(_statusCode)
            {
                Content = new StringContent(_responseBody, Encoding.UTF8, "application/json")
            };
        }
    }

    private const string SuccessJson = """
    {
        "content": [{ "type": "text", "text": "{\"sql\":\"SELECT 1\"}" }],
        "usage": { "input_tokens": 10, "output_tokens": 20 }
    }
    """;

    private static (AnthropicProvider provider, MockHttpHandler handler) CreateProvider(
        string responseJson = SuccessJson,
        HttpStatusCode statusCode = HttpStatusCode.OK)
    {
        var handler = new MockHttpHandler(responseJson, statusCode);
        var client = new HttpClient(handler);
        return (new AnthropicProvider(client, "test-key"), handler);
    }

    [Fact]
    public async Task Successful_Response_Should_Return_Text_And_Total_Tokens()
    {
        var (provider, _) = CreateProvider();

        var result = await provider.GenerateAsync("system prompt", "user prompt");

        result.Text.Should().Be("{\"sql\":\"SELECT 1\"}");
        result.TokensUsed.Should().Be(30); // input + output
    }

    [Fact]
    public async Task Request_Should_Target_Messages_Endpoint_With_Anthropic_Headers()
    {
        var (provider, handler) = CreateProvider();

        await provider.GenerateAsync("system", "user");

        handler.LastRequest!.RequestUri!.AbsoluteUri.Should().Be("https://api.anthropic.com/v1/messages");
        handler.LastRequest.Headers.GetValues("x-api-key").Should().ContainSingle("test-key");
        handler.LastRequest.Headers.GetValues("anthropic-version").Should().ContainSingle("2023-06-01");
    }

    [Fact]
    public async Task Request_Body_Should_Carry_System_And_User_Message()
    {
        var (provider, handler) = CreateProvider();

        await provider.GenerateAsync("the system prompt", "the user prompt");

        handler.LastRequestBody.Should().Contain("\"system\"").And.Contain("the system prompt");
        handler.LastRequestBody.Should().Contain("\"role\":\"user\"").And.Contain("the user prompt");
        handler.LastRequestBody.Should().Contain("\"max_tokens\"");
        // Newer Claude models reject sampling parameters — the provider must not send them
        handler.LastRequestBody.Should().NotContain("temperature");
    }

    [Fact]
    public async Task Rate_Limit_429_Should_Throw_With_Rate_Limit_Classification()
    {
        var (provider, _) = CreateProvider("""{"error":{"message":"rate limited"}}""", HttpStatusCode.TooManyRequests);

        var act = () => provider.GenerateAsync("system", "user");

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*rate limit*");
    }

    [Fact]
    public async Task Server_Error_Should_Throw_InvalidOperationException()
    {
        var (provider, _) = CreateProvider("""{"error":{"message":"overloaded"}}""", HttpStatusCode.InternalServerError);

        var act = () => provider.GenerateAsync("system", "user");

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Anthropic API error*");
    }

    [Fact]
    public async Task Network_Error_Should_Throw_InvalidOperationException()
    {
        var handler = new MockHttpHandler(new HttpRequestException("Connection refused"));
        var provider = new AnthropicProvider(new HttpClient(handler), "test-key");

        var act = () => provider.GenerateAsync("system", "user");

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Failed to connect*");
    }

    [Fact]
    public async Task Custom_Model_Should_Be_Sent_In_Body()
    {
        var handler = new MockHttpHandler(SuccessJson, HttpStatusCode.OK);
        var provider = new AnthropicProvider(new HttpClient(handler), "test-key", model: "claude-haiku-4-5");

        await provider.GenerateAsync("system", "user");

        handler.LastRequestBody.Should().Contain("claude-haiku-4-5");
    }
}

using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NaturalQuery.Models;

namespace NaturalQuery.Providers;

/// <summary>
/// LLM provider for the Anthropic Messages API (Claude models) via raw HttpClient —
/// no SDK dependency. Same response contract, token reporting, and error
/// classification as the other providers.
/// </summary>
public class AnthropicProvider : ILlmProvider
{
    private const string AnthropicVersion = "2023-06-01";

    private readonly HttpClient _httpClient;
    private readonly string _apiKey;
    private readonly string _model;
    private readonly int _maxTokens;
    private readonly ILogger<AnthropicProvider> _logger;

    /// <summary>
    /// Initializes the Anthropic provider.
    /// </summary>
    /// <param name="httpClient">HttpClient (base address defaults to https://api.anthropic.com/).</param>
    /// <param name="apiKey">Anthropic API key.</param>
    /// <param name="model">Model ID. Default: "claude-sonnet-5".</param>
    /// <param name="maxTokens">Maximum tokens for the response. Default: 1000.</param>
    /// <param name="logger">Logger instance.</param>
    public AnthropicProvider(
        HttpClient httpClient,
        string apiKey,
        string model = "claude-sonnet-5",
        int maxTokens = 1000,
        ILogger<AnthropicProvider>? logger = null)
    {
        _httpClient = httpClient;
        _apiKey = apiKey;
        _model = model;
        _maxTokens = maxTokens;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<AnthropicProvider>.Instance;

        if (_httpClient.BaseAddress == null)
            _httpClient.BaseAddress = new Uri("https://api.anthropic.com/");
    }

    /// <inheritdoc />
    public async Task<LlmResponse> GenerateAsync(string systemPrompt, string userPrompt, CancellationToken ct = default)
    {
        // Note: no sampling parameters (temperature/top_p) — current Claude models
        // reject them; behavior is steered via the system prompt.
        var request = new
        {
            model = _model,
            max_tokens = _maxTokens,
            system = systemPrompt,
            messages = new object[]
            {
                new { role = "user", content = userPrompt }
            }
        };

        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "v1/messages");
        httpRequest.Headers.Add("x-api-key", _apiKey);
        httpRequest.Headers.Add("anthropic-version", AnthropicVersion);
        httpRequest.Content = JsonContent.Create(request);

        HttpResponseMessage response;
        try
        {
            response = await _httpClient.SendAsync(httpRequest, ct);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "[Anthropic] Request failed");
            throw new InvalidOperationException("Failed to connect to Anthropic API.", ex);
        }

        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(ct);
            _logger.LogError("[Anthropic] Error {StatusCode}: {Body}", response.StatusCode, errorBody);

            if ((int)response.StatusCode == 429)
                throw new InvalidOperationException("Anthropic rate limit reached. Try again later.");

            throw new InvalidOperationException($"Anthropic API error ({response.StatusCode}): {errorBody}");
        }

        var json = await response.Content.ReadAsStringAsync(ct);
        var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        var text = "";
        if (root.TryGetProperty("content", out var content))
        {
            foreach (var block in content.EnumerateArray())
            {
                if (block.TryGetProperty("type", out var type) && type.GetString() == "text")
                {
                    text = block.GetProperty("text").GetString() ?? "";
                    break;
                }
            }
        }

        var inputTokens = root.GetProperty("usage").GetProperty("input_tokens").GetInt32();
        var outputTokens = root.GetProperty("usage").GetProperty("output_tokens").GetInt32();
        var totalTokens = inputTokens + outputTokens;

        _logger.LogInformation("[Anthropic] Response received. Model: {Model}, Tokens: {Tokens}", _model, totalTokens);

        return new LlmResponse(text, totalTokens);
    }
}

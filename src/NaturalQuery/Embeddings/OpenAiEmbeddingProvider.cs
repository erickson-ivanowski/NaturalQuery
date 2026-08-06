using System.Net.Http.Json;
using System.Text.Json;

namespace NaturalQuery.Embeddings;

/// <summary>
/// Embedding provider for the OpenAI (or OpenAI-compatible) embeddings API.
/// Raw HttpClient — no SDK dependency, following the same pattern as OpenAiProvider.
/// </summary>
public class OpenAiEmbeddingProvider : IEmbeddingProvider
{
    private readonly HttpClient _httpClient;
    private readonly string _apiKey;
    private readonly string _model;

    public OpenAiEmbeddingProvider(HttpClient httpClient, string apiKey, string model = "text-embedding-3-small")
    {
        _httpClient = httpClient;
        _apiKey = apiKey;
        _model = model;

        if (_httpClient.BaseAddress == null)
            _httpClient.BaseAddress = new Uri("https://api.openai.com/");
    }

    /// <inheritdoc />
    public async Task<float[]> EmbedAsync(string text, CancellationToken ct = default)
    {
        var request = new { model = _model, input = text };

        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "v1/embeddings");
        httpRequest.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _apiKey);
        httpRequest.Content = JsonContent.Create(request);

        var response = await _httpClient.SendAsync(httpRequest, ct);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(ct);
            throw new InvalidOperationException($"OpenAI embeddings API error ({response.StatusCode}): {body}");
        }

        var json = await response.Content.ReadAsStringAsync(ct);
        var doc = JsonDocument.Parse(json);
        var embeddingElement = doc.RootElement.GetProperty("data")[0].GetProperty("embedding");

        var result = new float[embeddingElement.GetArrayLength()];
        var i = 0;
        foreach (var value in embeddingElement.EnumerateArray())
            result[i++] = value.GetSingle();

        return result;
    }
}

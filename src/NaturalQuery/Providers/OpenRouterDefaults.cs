namespace NaturalQuery.Providers;

/// <summary>
/// OpenRouter connection defaults — OpenRouter exposes an OpenAI-compatible API,
/// so NaturalQuery reuses <see cref="OpenAiProvider"/> with a custom base address
/// plus OpenRouter's recommended attribution headers.
/// </summary>
public static class OpenRouterDefaults
{
    /// <summary>OpenRouter API base URL (OpenAI-compatible paths resolve under it).</summary>
    public const string BaseUrl = "https://openrouter.ai/api/";

    /// <summary>
    /// Creates an HttpClient targeting OpenRouter with optional attribution headers
    /// (HTTP-Referer and X-Title), as recommended by OpenRouter for app rankings.
    /// </summary>
    public static HttpClient CreateHttpClient(string? referer = null, string? title = null)
    {
        var client = new HttpClient { BaseAddress = new Uri(BaseUrl) };

        if (!string.IsNullOrEmpty(referer))
            client.DefaultRequestHeaders.Add("HTTP-Referer", referer);
        if (!string.IsNullOrEmpty(title))
            client.DefaultRequestHeaders.Add("X-Title", title);

        return client;
    }
}

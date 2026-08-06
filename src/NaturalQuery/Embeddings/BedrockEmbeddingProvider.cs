using System.Text;
using System.Text.Json;
using Amazon.BedrockRuntime;
using Amazon.BedrockRuntime.Model;

namespace NaturalQuery.Embeddings;

/// <summary>
/// Embedding provider using an Amazon Bedrock embedding model (e.g. Titan Embed).
/// Uses the existing AWSSDK.BedrockRuntime dependency already present in core.
/// </summary>
public class BedrockEmbeddingProvider : IEmbeddingProvider
{
    private readonly IAmazonBedrockRuntime _client;
    private readonly string _modelId;

    public BedrockEmbeddingProvider(IAmazonBedrockRuntime client, string modelId = "amazon.titan-embed-text-v2:0")
    {
        _client = client;
        _modelId = modelId;
    }

    /// <inheritdoc />
    public async Task<float[]> EmbedAsync(string text, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(new { inputText = text });

        var request = new InvokeModelRequest
        {
            ModelId = _modelId,
            ContentType = "application/json",
            Accept = "application/json",
            Body = new MemoryStream(Encoding.UTF8.GetBytes(payload))
        };

        var response = await _client.InvokeModelAsync(request, ct);

        using var reader = new StreamReader(response.Body);
        var json = await reader.ReadToEndAsync(ct);
        var doc = JsonDocument.Parse(json);
        var embeddingElement = doc.RootElement.GetProperty("embedding");

        var result = new float[embeddingElement.GetArrayLength()];
        var i = 0;
        foreach (var value in embeddingElement.EnumerateArray())
            result[i++] = value.GetSingle();

        return result;
    }
}

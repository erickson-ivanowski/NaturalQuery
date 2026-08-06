namespace NaturalQuery.Embeddings;

/// <summary>
/// Produces vector embeddings for text, used by the opt-in semantic cache to
/// measure similarity between questions.
/// </summary>
public interface IEmbeddingProvider
{
    /// <summary>Embeds the given text into a vector.</summary>
    Task<float[]> EmbedAsync(string text, CancellationToken ct = default);
}

using NaturalQuery.Embeddings;
using NaturalQuery.Models;

namespace NaturalQuery.Caching;

/// <summary>
/// In-memory embedding-similarity cache. Tenant-scoped, TTL-honoring (same expiry
/// rules as the exact-match cache), and conservative by design: a lookup falls
/// through to a fresh AI call on miss or on any embedding-provider error.
/// </summary>
public class SemanticQueryCache : ISemanticQueryCache
{
    private sealed record Entry(string TenantKey, float[] Embedding, QueryResult Result, DateTime ExpiresAtUtc);

    private readonly IEmbeddingProvider _embeddingProvider;
    private readonly double _similarityThreshold;
    private readonly TimeSpan _ttl;
    private readonly List<Entry> _entries = new();
    private readonly object _lock = new();

    public SemanticQueryCache(IEmbeddingProvider embeddingProvider, double similarityThreshold, int ttlMinutes)
    {
        _embeddingProvider = embeddingProvider;
        _similarityThreshold = similarityThreshold;
        _ttl = TimeSpan.FromMinutes(ttlMinutes);
    }

    /// <inheritdoc />
    public async Task<QueryResult?> GetSimilarAsync(string question, string? tenantId, CancellationToken ct = default)
    {
        float[] embedding;
        try
        {
            embedding = await _embeddingProvider.EmbedAsync(question, ct);
        }
        catch
        {
            return null; // fall through to a fresh AI call
        }

        var tenantKey = tenantId ?? "global";
        var now = DateTime.UtcNow;

        lock (_lock)
        {
            _entries.RemoveAll(e => e.ExpiresAtUtc <= now);

            Entry? best = null;
            var bestScore = -1.0;

            foreach (var entry in _entries)
            {
                if (entry.TenantKey != tenantKey) continue; // strict tenant scoping

                var score = CosineSimilarity(embedding, entry.Embedding);
                if (score > bestScore)
                {
                    bestScore = score;
                    best = entry;
                }
            }

            return best != null && bestScore >= _similarityThreshold ? best.Result : null;
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(string question, string? tenantId, QueryResult result, CancellationToken ct = default)
    {
        float[] embedding;
        try
        {
            embedding = await _embeddingProvider.EmbedAsync(question, ct);
        }
        catch
        {
            return; // best-effort: skip caching on embedding failure
        }

        var entry = new Entry(tenantId ?? "global", embedding, result, DateTime.UtcNow + _ttl);
        lock (_lock)
        {
            _entries.Add(entry);
        }
    }

    private static double CosineSimilarity(float[] a, float[] b)
    {
        double dot = 0, magA = 0, magB = 0;
        var len = Math.Min(a.Length, b.Length);
        for (var i = 0; i < len; i++)
        {
            dot += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }
        if (magA == 0 || magB == 0) return 0;
        return dot / (Math.Sqrt(magA) * Math.Sqrt(magB));
    }
}

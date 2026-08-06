using FluentAssertions;
using NaturalQuery.Caching;
using NaturalQuery.Embeddings;
using NaturalQuery.Models;

namespace NaturalQuery.Tests;

/// <summary>
/// FR-030: opt-in semantic cache — reuses results for questions with equivalent
/// meaning, scoped strictly per tenant, with a conservative similarity threshold.
/// </summary>
public class SemanticCacheTests
{
    /// <summary>
    /// Deterministic fake embedding for testing similarity logic without a real
    /// model: a small fixed vocabulary of concept axes, where synonyms map to the
    /// same axis (paraphrases score high) and antonyms map to opposite signs on a
    /// dedicated axis (antonym pairs score low), plus a shared "topic" axis so
    /// same-topic sentences are close and different-topic sentences are not.
    /// </summary>
    private sealed class FakeEmbeddingProvider : IEmbeddingProvider
    {
        private static readonly Dictionary<string, int> RankingConcept = new()
        {
            ["top"] = 1, ["best"] = 1, ["highest"] = 1, ["selling"] = 1, ["sales"] = 1,
            ["worst"] = -1, ["least"] = -1, ["bottom"] = -1,
        };

        public Task<float[]> EmbedAsync(string text, CancellationToken ct = default)
        {
            var words = text.ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            // [topic axis, ranking-direction axis]
            var vector = new float[2];
            vector[0] = 1; // shared topic: all these sentences are about "products"

            foreach (var word in words)
            {
                if (RankingConcept.TryGetValue(word, out var sign))
                    vector[1] += sign;
            }

            return Task.FromResult(vector);
        }
    }

    private SemanticQueryCache CreateCache(double threshold = 0.97) =>
        new(new FakeEmbeddingProvider(), threshold, ttlMinutes: 5);

    [Fact]
    public async Task Paraphrase_Pair_Should_Hit_Cache()
    {
        var cache = CreateCache(threshold: 0.9);
        var result = new QueryResult { Sql = "SELECT * FROM products ORDER BY sales DESC", Title = "top" };
        await cache.SetAsync("top products by sales", "tenant-1", result);

        var hit = await cache.GetSimilarAsync("best selling products by sales", "tenant-1");

        hit.Should().NotBeNull();
        hit!.Sql.Should().Be(result.Sql);
    }

    [Fact]
    public async Task Antonym_Pair_Should_Not_Hit_Cache()
    {
        var cache = CreateCache(threshold: 0.97);
        var result = new QueryResult { Sql = "SELECT * FROM products ORDER BY sales DESC LIMIT 10" };
        await cache.SetAsync("top products by sales", "tenant-1", result);

        var hit = await cache.GetSimilarAsync("worst products by sales", "tenant-1");

        hit.Should().BeNull();
    }

    [Fact]
    public async Task Different_Tenant_Should_Never_Share_Cache()
    {
        var cache = CreateCache(threshold: 0.5); // permissive threshold to isolate the tenant-scoping check
        var result = new QueryResult { Sql = "SELECT * FROM products" };
        await cache.SetAsync("top products by sales", "tenant-a", result);

        var hit = await cache.GetSimilarAsync("top products by sales", "tenant-b");

        hit.Should().BeNull();
    }

    [Fact]
    public async Task Exact_Repeat_Should_Always_Hit()
    {
        var cache = CreateCache();
        var result = new QueryResult { Sql = "SELECT * FROM products" };
        await cache.SetAsync("top products by sales", "tenant-1", result);

        var hit = await cache.GetSimilarAsync("top products by sales", "tenant-1");

        hit.Should().NotBeNull();
    }

    [Fact]
    public async Task Expired_Entry_Should_Not_Be_Returned()
    {
        var cache = new SemanticQueryCache(new FakeEmbeddingProvider(), similarityThreshold: 0.9, ttlMinutes: 0);
        var result = new QueryResult { Sql = "SELECT * FROM products" };
        await cache.SetAsync("top products by sales", "tenant-1", result);

        await Task.Delay(50);
        var hit = await cache.GetSimilarAsync("top products by sales", "tenant-1");

        hit.Should().BeNull();
    }

    [Fact]
    public async Task Miss_Should_Return_Null_Without_Throwing()
    {
        var cache = CreateCache();

        var hit = await cache.GetSimilarAsync("anything at all", "tenant-1");

        hit.Should().BeNull();
    }
}

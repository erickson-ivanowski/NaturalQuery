using NaturalQuery.Models;

namespace NaturalQuery.Caching;

/// <summary>
/// Opt-in cache that reuses results for questions with equivalent meaning,
/// strictly scoped per tenant. A conservative similarity threshold ensures a
/// wrong reuse is treated as a defect, not a tradeoff.
/// </summary>
public interface ISemanticQueryCache
{
    /// <summary>Finds a cached result for a semantically similar question, or null.</summary>
    Task<QueryResult?> GetSimilarAsync(string question, string? tenantId, CancellationToken ct = default);

    /// <summary>Stores a result for future semantic lookups.</summary>
    Task SetAsync(string question, string? tenantId, QueryResult result, CancellationToken ct = default);
}

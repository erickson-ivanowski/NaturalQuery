using System.Diagnostics.Metrics;

namespace NaturalQuery.Diagnostics;

/// <summary>
/// Native metrics for NaturalQuery — query count, latency, token usage, cache
/// hit/miss, and error rate — observable through any standard .NET metrics
/// pipeline (OpenTelemetry, dotnet-counters, etc.) via the "NaturalQuery" Meter.
/// </summary>
public static class NaturalQueryMetrics
{
    /// <summary>Meter name consumed by metrics exporters.</summary>
    public const string MeterName = "NaturalQuery";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    private static readonly Counter<long> Queries =
        Meter.CreateCounter<long>("naturalquery.queries", description: "Number of processed questions, tagged by outcome and tenant.");

    private static readonly Counter<long> Tokens =
        Meter.CreateCounter<long>("naturalquery.tokens", description: "LLM tokens consumed.");

    private static readonly Counter<long> Cache =
        Meter.CreateCounter<long>("naturalquery.cache", description: "Cache lookups, tagged hit/miss.");

    private static readonly Histogram<double> Duration =
        Meter.CreateHistogram<double>("naturalquery.duration", unit: "ms", description: "End-to-end request duration.");

    /// <summary>Records one processed question.</summary>
    public static void RecordQuery(string outcome, string? tenantId, long elapsedMs, int tokensUsed)
    {
        var tenantTag = tenantId ?? "none";
        Queries.Add(1, new KeyValuePair<string, object?>("outcome", outcome), new KeyValuePair<string, object?>("tenant", tenantTag));
        Duration.Record(elapsedMs, new KeyValuePair<string, object?>("outcome", outcome), new KeyValuePair<string, object?>("tenant", tenantTag));
        if (tokensUsed > 0)
            Tokens.Add(tokensUsed, new KeyValuePair<string, object?>("tenant", tenantTag));
    }

    /// <summary>Records a cache lookup result.</summary>
    public static void RecordCache(bool hit, string? tenantId = null) =>
        Cache.Add(1,
            new KeyValuePair<string, object?>("result", hit ? "hit" : "miss"),
            new KeyValuePair<string, object?>("tenant", tenantId ?? "none"));
}

using Microsoft.Extensions.Diagnostics.HealthChecks;
using NaturalQuery.Providers;

namespace NaturalQuery.Health;

/// <summary>
/// Reports NaturalQuery's dependency reachability: the query executor (database)
/// and the configured LLM provider (presence only — no billable AI call is made).
/// </summary>
public class NaturalQueryHealthCheck : IHealthCheck
{
    private readonly IQueryExecutor _executor;
    private readonly ILlmProvider _llmProvider;

    public NaturalQueryHealthCheck(IQueryExecutor executor, ILlmProvider llmProvider)
    {
        _executor = executor;
        _llmProvider = llmProvider;
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        try
        {
            // A trivial read proves the executor can reach the database without
            // depending on any specific table existing.
            await _executor.ExecuteTableQueryAsync("SELECT 1", ct);
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("NaturalQuery database is unreachable.", ex);
        }

        // The LLM provider is checked for presence only (DI would already have
        // failed to resolve it if unconfigured); no billable call is issued here.
        if (_llmProvider == null)
            return HealthCheckResult.Unhealthy("NaturalQuery has no AI provider configured.");

        return HealthCheckResult.Healthy("NaturalQuery database and AI provider are reachable.");
    }
}

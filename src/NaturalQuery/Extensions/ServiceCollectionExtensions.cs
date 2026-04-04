using Amazon.Athena;
using Amazon.BedrockRuntime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NaturalQuery.Caching;
using NaturalQuery.Diagnostics;
using NaturalQuery.Discovery;
using NaturalQuery.Providers;
using NaturalQuery.RateLimiting;

namespace NaturalQuery.Extensions;

/// <summary>
/// Extension methods for registering NaturalQuery services in the DI container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers NaturalQuery services with the given configuration.
    /// Returns a builder for chaining provider and feature configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action to configure NaturalQueryOptions.</param>
    /// <returns>A builder for fluent provider configuration.</returns>
    public static NaturalQueryBuilder AddNaturalQuery(
        this IServiceCollection services,
        Action<NaturalQueryOptions> configure)
    {
        services.Configure(configure);
        services.AddScoped<INaturalQueryEngine, NaturalQueryEngine>();
        return new NaturalQueryBuilder(services);
    }
}

/// <summary>
/// Fluent builder for configuring NaturalQuery providers, caching, rate limiting, and diagnostics.
/// </summary>
public class NaturalQueryBuilder
{
    private readonly IServiceCollection _services;

    /// <summary>Creates a new builder wrapping the service collection.</summary>
    public NaturalQueryBuilder(IServiceCollection services)
    {
        _services = services;
    }

    // ── LLM Providers ───────────────────────────────────────────

    /// <summary>
    /// Use AWS Bedrock (Claude) as the LLM provider.
    /// Requires IAmazonBedrockRuntime to be registered in the DI container.
    /// </summary>
    /// <param name="modelId">Bedrock model ID (e.g., "us.anthropic.claude-haiku-4-5-20251001-v1:0").</param>
    public NaturalQueryBuilder UseBedrockProvider(string modelId)
    {
        _services.AddSingleton<ILlmProvider>(sp =>
        {
            var client = sp.GetRequiredService<IAmazonBedrockRuntime>();
            var options = sp.GetRequiredService<IOptions<NaturalQueryOptions>>();
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<BedrockProvider>>();
            return new BedrockProvider(client, modelId, options, logger);
        });
        return this;
    }

    /// <summary>
    /// Use OpenAI (or any OpenAI-compatible API) as the LLM provider.
    /// No SDK dependency — uses raw HttpClient.
    /// </summary>
    /// <param name="apiKey">API key for authentication.</param>
    /// <param name="model">Model name. Default: "gpt-4o-mini".</param>
    /// <param name="baseUrl">Base URL for the API. Default: "https://api.openai.com/".</param>
    public NaturalQueryBuilder UseOpenAiProvider(string apiKey, string model = "gpt-4o-mini", string? baseUrl = null)
    {
        _services.AddSingleton<ILlmProvider>(sp =>
        {
            var httpClient = new HttpClient();
            if (!string.IsNullOrEmpty(baseUrl))
                httpClient.BaseAddress = new Uri(baseUrl);

            var options = sp.GetRequiredService<IOptions<NaturalQueryOptions>>().Value;
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<OpenAiProvider>>();
            return new OpenAiProvider(httpClient, apiKey, model, options.MaxTokens, options.Temperature, logger);
        });
        return this;
    }

    /// <summary>Use a custom LLM provider implementation.</summary>
    public NaturalQueryBuilder UseLlmProvider<T>() where T : class, ILlmProvider
    {
        _services.AddScoped<ILlmProvider, T>();
        return this;
    }

    // ── Query Executors ─────────────────────────────────────────

    /// <summary>
    /// Use Amazon Athena as the query executor.
    /// Requires IAmazonAthena to be registered in the DI container.
    /// </summary>
    /// <param name="database">Athena/Glue database name.</param>
    /// <param name="workgroup">Athena workgroup name.</param>
    /// <param name="outputLocation">S3 location for query results.</param>
    /// <param name="timeoutSeconds">Query timeout in seconds. Default: 30.</param>
    public NaturalQueryBuilder UseAthenaExecutor(string database, string workgroup, string outputLocation, int timeoutSeconds = 30)
    {
        _services.AddSingleton<IQueryExecutor>(sp =>
        {
            var client = sp.GetRequiredService<IAmazonAthena>();
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<AthenaQueryExecutor>>();
            return new AthenaQueryExecutor(client, database, workgroup, outputLocation, logger, timeoutSeconds);
        });
        return this;
    }

    /// <summary>
    /// Use PostgreSQL as the query executor.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string.</param>
    /// <param name="timeoutSeconds">Command timeout in seconds. Default: 30.</param>
    /// <param name="wrapInTransaction">
    /// When true, wraps every query in BEGIN + ROLLBACK as an extra safety layer.
    /// Prevents accidental writes even if SQL validation is bypassed. Default: false.
    /// </param>
    public NaturalQueryBuilder UsePostgresExecutor(string connectionString, int timeoutSeconds = 30, bool wrapInTransaction = false)
    {
        _services.AddSingleton<IQueryExecutor>(sp =>
        {
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgresQueryExecutor>>();
            return new PostgresQueryExecutor(connectionString, logger, timeoutSeconds, wrapInTransaction);
        });
        return this;
    }

    /// <summary>Use a custom query executor implementation.</summary>
    public NaturalQueryBuilder UseQueryExecutor<T>() where T : class, IQueryExecutor
    {
        _services.AddScoped<IQueryExecutor, T>();
        return this;
    }

    // ── Caching ─────────────────────────────────────────────────

    /// <summary>
    /// Enable in-memory query caching. TTL is configured via NaturalQueryOptions.CacheTtlMinutes.
    /// </summary>
    public NaturalQueryBuilder UseInMemoryCache()
    {
        _services.AddMemoryCache();
        _services.AddSingleton<IQueryCache, InMemoryQueryCache>();
        return this;
    }

    /// <summary>Use a custom query cache implementation.</summary>
    public NaturalQueryBuilder UseQueryCache<T>() where T : class, IQueryCache
    {
        _services.AddSingleton<IQueryCache, T>();
        return this;
    }

    // ── Rate Limiting ───────────────────────────────────────────

    /// <summary>
    /// Enable in-memory rate limiting. Limit is configured via NaturalQueryOptions.RateLimitPerMinute.
    /// </summary>
    public NaturalQueryBuilder UseInMemoryRateLimiter()
    {
        _services.AddSingleton<IRateLimiter, InMemoryRateLimiter>();
        return this;
    }

    /// <summary>Use a custom rate limiter implementation.</summary>
    public NaturalQueryBuilder UseRateLimiter<T>() where T : class, IRateLimiter
    {
        _services.AddSingleton<IRateLimiter, T>();
        return this;
    }

    // ── Error Handling ──────────────────────────────────────────

    /// <summary>
    /// Register an error handler for tracking NL2SQL failures.
    /// </summary>
    public NaturalQueryBuilder UseErrorHandler<T>() where T : class, IErrorHandler
    {
        _services.AddSingleton<IErrorHandler, T>();
        return this;
    }

    /// <summary>
    /// Register an inline error handler using a callback action.
    /// </summary>
    /// <param name="handler">Async callback invoked on each error.</param>
    public NaturalQueryBuilder UseErrorHandler(Func<NaturalQueryError, CancellationToken, Task> handler)
    {
        _services.AddSingleton<IErrorHandler>(new DelegateErrorHandler(handler));
        return this;
    }

    // ── Schema Discovery ────────────────────────────────────────

    /// <summary>Use a custom schema discovery implementation.</summary>
    public NaturalQueryBuilder UseSchemaDiscovery<T>() where T : class, ISchemaDiscovery
    {
        _services.AddScoped<ISchemaDiscovery, T>();
        return this;
    }

    /// <summary>
    /// Use PostgreSQL schema discovery (reads from information_schema).
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string.</param>
    public NaturalQueryBuilder UsePostgresSchemaDiscovery(string connectionString)
    {
        _services.AddSingleton<ISchemaDiscovery>(sp =>
        {
            var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<PostgresSchemaDiscovery>>();
            return new PostgresSchemaDiscovery(connectionString, logger);
        });
        return this;
    }

    // ── Cost Estimation ─────────────────────────────────────────

    /// <summary>Use a custom query cost estimator.</summary>
    public NaturalQueryBuilder UseQueryCostEstimator<T>() where T : class, IQueryCostEstimator
    {
        _services.AddSingleton<IQueryCostEstimator, T>();
        return this;
    }

    // ── Internal helpers ────────────────────────────────────────

    private class DelegateErrorHandler : IErrorHandler
    {
        private readonly Func<NaturalQueryError, CancellationToken, Task> _handler;

        public DelegateErrorHandler(Func<NaturalQueryError, CancellationToken, Task> handler) =>
            _handler = handler;

        public Task HandleAsync(NaturalQueryError error, CancellationToken ct) =>
            _handler(error, ct);
    }
}

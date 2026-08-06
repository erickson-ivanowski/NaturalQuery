using NaturalQuery.Models;
using NaturalQuery.Security;

namespace NaturalQuery;

/// <summary>
/// Configuration options for the NaturalQuery engine.
/// </summary>
public class NaturalQueryOptions
{
    /// <summary>Database table schemas available for querying.</summary>
    public List<TableSchema> Tables { get; set; } = new();

    /// <summary>
    /// Placeholder string in generated SQL that will be replaced by the actual tenant ID.
    /// Example: "{TENANT_ID}"
    /// </summary>
    public string? TenantIdPlaceholder { get; set; }

    /// <summary>
    /// Column name used for tenant isolation (e.g., "tenant_id", "clientid").
    /// When set, all queries MUST include a WHERE filter on this column.
    /// </summary>
    public string? TenantIdColumn { get; set; }

    /// <summary>Maximum tokens for LLM response. Default: 1000.</summary>
    public int MaxTokens { get; set; } = 1000;

    /// <summary>LLM temperature (0.0 = deterministic, 1.0 = creative). Default: 0.1.</summary>
    public double Temperature { get; set; } = 0.1;

    /// <summary>
    /// Override the entire system prompt. When set, the auto-generated schema prompt is ignored.
    /// Use {TABLES_SCHEMA} placeholder to inject the auto-generated schema into a custom prompt.
    /// </summary>
    public string? CustomSystemPrompt { get; set; }

    /// <summary>Cache time-to-live in minutes. Default: 5. Set to 0 to disable caching.</summary>
    public int CacheTtlMinutes { get; set; } = 5;

    /// <summary>Maximum requests per minute per tenant for rate limiting. Default: 60.</summary>
    public int RateLimitPerMinute { get; set; } = 60;

    /// <summary>Additional SQL keywords to block (beyond the built-in list).</summary>
    public List<string> ForbiddenSqlKeywords { get; set; } = new();

    /// <summary>
    /// Additional rules/instructions to append to the system prompt.
    /// Useful for domain-specific guidance without replacing the entire prompt.
    /// </summary>
    public List<string> AdditionalRules { get; set; } = new();

    /// <summary>
    /// Maximum number of retry attempts when a generated query fails to execute.
    /// The engine sends the error back to the LLM and asks it to fix the SQL.
    /// Default: 0 (no retries). Maximum: 3.
    /// </summary>
    public int MaxRetries { get; set; } = 0;

    /// <summary>Maximum question length in characters. Oversized questions are rejected before any AI call. Default: 2000.</summary>
    public int MaxQuestionLength { get; set; } = 2000;

    /// <summary>Maximum number of conversation-history turns accepted from the caller. Default: 20.</summary>
    public int MaxContextTurns { get; set; } = 20;

    /// <summary>Maximum result row count; results exceeding it are truncated and marked. Default: 10000.</summary>
    public int MaxResultRows { get; set; } = 10000;

    /// <summary>Query execution timeout in seconds; database work is cancelled when exceeded. Default: 30.</summary>
    public int QueryTimeoutSeconds { get; set; } = 30;

    /// <summary>
    /// Regex policy a tenant identifier must fully match before any use.
    /// Default allows letters, digits, dot, hyphen, underscore, up to 128 characters.
    /// </summary>
    public string TenantIdPattern { get; set; } = "^[A-Za-z0-9._-]{1,128}$";

    /// <summary>Prompt-injection screening mode. Default: Warn (log + flag, never refuse).</summary>
    public InjectionScreeningMode InjectionScreening { get; set; } = InjectionScreeningMode.Warn;

    /// <summary>Additional prompt-injection regex patterns extending the built-in set.</summary>
    public List<string> InjectionPatterns { get; set; } = new();

    /// <summary>
    /// Cosine-similarity threshold for the opt-in semantic cache. Must be in (0.5, 1.0].
    /// Conservative by default: wrong reuse is a defect. Default: 0.97.
    /// </summary>
    public double SemanticCacheSimilarityThreshold { get; set; } = 0.97;
}

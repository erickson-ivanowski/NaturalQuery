namespace NaturalQuery.Auditing;

/// <summary>
/// One entry per processed question: who asked what, what ran, and what happened.
/// </summary>
public class AuditRecord
{
    /// <summary>The natural language question as received.</summary>
    public string Question { get; set; } = string.Empty;

    /// <summary>The generated SQL query (null when generation failed).</summary>
    public string? Sql { get; set; }

    /// <summary>The validated tenant identifier (null for single-tenant use).</summary>
    public string? TenantId { get; set; }

    /// <summary>
    /// Outcome classification: success, validation_rejected, injection_flagged,
    /// rate_limited, timeout, execution_error, or llm_error.
    /// </summary>
    public string Outcome { get; set; } = string.Empty;

    /// <summary>End-to-end duration in milliseconds.</summary>
    public long DurationMs { get; set; }

    /// <summary>LLM tokens consumed (0 when no AI call was made).</summary>
    public int TokensUsed { get; set; }

    /// <summary>UTC timestamp of record creation.</summary>
    public DateTime TimestampUtc { get; set; }

    /// <summary>Correlation identifier matching QueryResult.CorrelationId and server logs.</summary>
    public string CorrelationId { get; set; } = string.Empty;

    /// <summary>True when the result was truncated at the configured row cap.</summary>
    public bool Truncated { get; set; }
}

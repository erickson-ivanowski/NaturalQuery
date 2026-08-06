namespace NaturalQuery.Extensions;

/// <summary>
/// Opt-in protections for the mapped NaturalQuery endpoints: input size limits,
/// safe error responses with correlation IDs, and host-delegated authorization.
/// </summary>
public class NaturalQueryEndpointOptions
{
    /// <summary>
    /// When true, the endpoints require an authenticated user via the host
    /// application's authorization pipeline. The library manages no credentials.
    /// </summary>
    public bool RequireAuthorization { get; set; }

    /// <summary>
    /// Optional named authorization policy (host-defined) applied to the endpoints.
    /// Implies authorization is required when set.
    /// </summary>
    public string? AuthorizationPolicy { get; set; }

    /// <summary>
    /// Maximum question length in characters. Null falls back to
    /// <see cref="NaturalQueryOptions.MaxQuestionLength"/> (default 2000).
    /// </summary>
    public int? MaxQuestionLength { get; set; }

    /// <summary>
    /// Maximum conversation-history turns accepted. Null falls back to
    /// <see cref="NaturalQueryOptions.MaxContextTurns"/> (default 20).
    /// </summary>
    public int? MaxContextTurns { get; set; }
}

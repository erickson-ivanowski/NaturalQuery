namespace NaturalQuery.Auditing;

/// <summary>
/// Opt-in audit trail extension point. When registered, the engine writes exactly
/// one <see cref="AuditRecord"/> per processed question (success or failure).
/// A sink failure never fails the user's request — it is logged server-side.
/// </summary>
public interface IAuditSink
{
    /// <summary>Persists one audit record.</summary>
    Task WriteAsync(AuditRecord record, CancellationToken ct = default);
}

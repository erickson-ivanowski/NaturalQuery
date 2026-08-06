using System.Text.RegularExpressions;

namespace NaturalQuery.Validation;

/// <summary>
/// Validates tenant identifiers against a safe character policy before any use.
/// The tenant ID is untrusted input (it can arrive from an HTTP request) and must
/// never carry query syntax into the pipeline.
/// </summary>
public static class TenantIdValidator
{
    /// <summary>Default policy: letters, digits, dot, hyphen, underscore; 1–128 chars.</summary>
    public const string DefaultPattern = "^[A-Za-z0-9._-]{1,128}$";

    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Validates a tenant identifier against the policy pattern.
    /// Null or empty is treated as "not provided" and is valid (single-tenant use).
    /// The pattern must match the ENTIRE identifier — partial matches are rejected.
    /// </summary>
    /// <returns>Null if valid (or not provided); an error message otherwise.</returns>
    public static string? Validate(string? tenantId, string pattern = DefaultPattern)
    {
        if (string.IsNullOrEmpty(tenantId))
            return null; // not provided

        // Hard structural guard, independent of the configurable pattern (defense in depth):
        // quote characters, comment markers, and statement separators are never acceptable
        // in a tenant identifier, even under a permissive operator-supplied policy.
        if (tenantId.Contains('\'') || tenantId.Contains('"') || tenantId.Contains(';') ||
            tenantId.Contains("--") || tenantId.Contains("/*") || tenantId.Contains("*/") ||
            tenantId.Any(char.IsControl))
        {
            return "Tenant identifier contains forbidden query syntax.";
        }

        try
        {
            var match = Regex.Match(tenantId, pattern, RegexOptions.None, MatchTimeout);
            if (!match.Success || match.Index != 0 || match.Length != tenantId.Length)
                return "Tenant identifier does not conform to the configured policy.";
        }
        catch (RegexMatchTimeoutException)
        {
            return "Tenant identifier validation timed out.";
        }
        catch (ArgumentException)
        {
            // Broken operator-supplied pattern: fail closed.
            return "Tenant identifier policy pattern is invalid.";
        }

        return null;
    }
}

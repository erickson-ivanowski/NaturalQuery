using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;

namespace NaturalQuery;

/// <summary>
/// Fail-fast startup validation for <see cref="NaturalQueryOptions"/>. Only
/// impossible configurations fail; everything that currently works keeps working
/// (FR-028).
/// </summary>
public class NaturalQueryOptionsValidator : IValidateOptions<NaturalQueryOptions>
{
    public ValidateOptionsResult Validate(string? name, NaturalQueryOptions options)
    {
        var hasColumn = !string.IsNullOrEmpty(options.TenantIdColumn);
        var hasPlaceholder = !string.IsNullOrEmpty(options.TenantIdPlaceholder);

        if (hasColumn && !hasPlaceholder)
            return ValidateOptionsResult.Fail("NaturalQueryOptions.TenantIdColumn is set but TenantIdPlaceholder is not — both must be configured together, or neither (single-tenant mode).");

        if (hasPlaceholder && !hasColumn)
            return ValidateOptionsResult.Fail("NaturalQueryOptions.TenantIdColumn is not set but TenantIdPlaceholder is — both must be configured together, or neither (single-tenant mode).");

        if (options.MaxQuestionLength <= 0)
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.MaxQuestionLength must be positive (got {options.MaxQuestionLength}).");

        if (options.MaxContextTurns <= 0)
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.MaxContextTurns must be positive (got {options.MaxContextTurns}).");

        if (options.MaxResultRows <= 0)
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.MaxResultRows must be positive (got {options.MaxResultRows}).");

        if (options.QueryTimeoutSeconds <= 0)
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.QueryTimeoutSeconds must be positive (got {options.QueryTimeoutSeconds}).");

        if (options.SemanticCacheSimilarityThreshold <= 0.5 || options.SemanticCacheSimilarityThreshold > 1.0)
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.SemanticCacheSimilarityThreshold must be in the range (0.5, 1.0] (got {options.SemanticCacheSimilarityThreshold}).");

        try
        {
            _ = new Regex(options.TenantIdPattern);
        }
        catch (ArgumentException ex)
        {
            return ValidateOptionsResult.Fail($"NaturalQueryOptions.TenantIdPattern is not a valid regular expression: {ex.Message}");
        }

        return ValidateOptionsResult.Success;
    }
}

namespace NaturalQuery.Security;

/// <summary>
/// Controls how prompt-injection screening reacts to a flagged question or conversation turn.
/// </summary>
public enum InjectionScreeningMode
{
    /// <summary>Screening disabled entirely.</summary>
    Off,

    /// <summary>Detection on: log a warning and flag the result, but never refuse. Default.</summary>
    Warn,

    /// <summary>Strict mode: flagged questions are refused before any AI call.</summary>
    Block
}

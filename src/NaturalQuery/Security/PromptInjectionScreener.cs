using System.Text.RegularExpressions;

namespace NaturalQuery.Security;

/// <summary>
/// Best-effort detection of prompt-injection (instruction-override) patterns in
/// user questions and conversation turns. Detection is advisory: the hard guarantee
/// remains output-side SQL validation. Patterns cover English, Portuguese, and
/// Spanish and are operator-extensible.
/// </summary>
public class PromptInjectionScreener
{
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(250);

    private static readonly string[] BuiltInPatterns =
    {
        // English
        @"\bignore\s+(all\s+|your\s+|the\s+|previous\s+|prior\s+|any\s+)*(instructions?|rules?|prompts?)\b",
        @"\bdisregard\s+(all\s+|your\s+|the\s+|previous\s+|prior\s+)*(instructions?|rules?|prompts?)\b",
        @"\bforget\s+(all\s+|your\s+|the\s+|previous\s+|prior\s+)*(instructions?|rules?|prompts?)\b",
        @"\boverride\s+(all\s+|your\s+|the\s+)*.{0,20}?(safety|security|instructions?|rules?)\b",
        @"\bsystem\s*prompt\b",
        @"\byou\s+are\s+no\s+longer\b",
        @"\bpretend\s+(to\s+be|you\s+are)\b",
        @"\bjailbreak\b",
        @"\bnew\s+instructions?\s*:",
        // Portuguese
        @"\bignore\s+(as\s+|todas\s+as\s+|suas\s+|essas\s+)*(instruç(ões|oes)|regras?)\b",
        @"\besqueç?a\s+(as\s+|todas\s+as\s+|suas\s+)*(instruç(ões|oes)|regras?)\b",
        @"\bdesconsidere\s+(as\s+|todas\s+as\s+|suas\s+)*(instruç(ões|oes)|regras?)\b",
        // Spanish
        @"\bignora\s+(las\s+|todas\s+las\s+|tus\s+)*(instrucciones|reglas)\b",
        @"\bolvida\s+(las\s+|todas\s+las\s+|tus\s+)*(instrucciones|reglas)\b",
    };

    private readonly List<Regex> _patterns;

    /// <summary>
    /// Creates a screener with the built-in pattern set plus optional operator-supplied
    /// regex patterns. Invalid operator patterns are skipped (built-ins always apply).
    /// </summary>
    public PromptInjectionScreener(IEnumerable<string>? additionalPatterns = null)
    {
        _patterns = new List<Regex>(BuiltInPatterns.Length);

        foreach (var pattern in BuiltInPatterns)
            _patterns.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, MatchTimeout));

        if (additionalPatterns != null)
        {
            foreach (var pattern in additionalPatterns)
            {
                try
                {
                    _patterns.Add(new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, MatchTimeout));
                }
                catch (ArgumentException)
                {
                    // Broken operator pattern: skip — never disable the built-in screen.
                }
            }
        }
    }

    /// <summary>
    /// Returns true when the text matches a known instruction-override pattern.
    /// </summary>
    /// <param name="text">Question or conversation-turn text to screen.</param>
    /// <param name="matchedPattern">The pattern that fired, for logging/telemetry.</param>
    public bool IsSuspicious(string text, out string? matchedPattern)
    {
        matchedPattern = null;
        if (string.IsNullOrEmpty(text))
            return false;

        foreach (var regex in _patterns)
        {
            try
            {
                if (regex.IsMatch(text))
                {
                    matchedPattern = regex.ToString();
                    return true;
                }
            }
            catch (RegexMatchTimeoutException)
            {
                // Treat a timed-out pattern as suspicious: adversarial input shapes
                // are exactly what cause catastrophic matching.
                matchedPattern = regex.ToString();
                return true;
            }
        }

        return false;
    }
}

using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NaturalQuery.Auditing;
using NaturalQuery.Caching;
using NaturalQuery.Masking;
using NaturalQuery.Diagnostics;
using NaturalQuery.Models;
using NaturalQuery.Providers;
using NaturalQuery.RateLimiting;
using NaturalQuery.Security;
using NaturalQuery.Validation;

namespace NaturalQuery;

/// <summary>
/// Core NL2SQL engine. Converts natural language questions into SQL queries,
/// validates them, executes them, and returns structured results.
/// Supports caching, rate limiting, conversation context, diagnostics, and error handling.
/// </summary>
public class NaturalQueryEngine : INaturalQueryEngine
{
    private readonly ILlmProvider _llmProvider;
    private readonly IQueryExecutor _queryExecutor;
    private readonly NaturalQueryOptions _options;
    private readonly ILogger<NaturalQueryEngine> _logger;
    private readonly IQueryCache? _cache;
    private readonly IRateLimiter? _rateLimiter;
    private readonly IErrorHandler? _errorHandler;
    private readonly IAuditSink? _auditSink;
    private readonly PromptInjectionScreener _injectionScreener;

    /// <summary>
    /// Initializes the NaturalQuery engine with all dependencies.
    /// Cache, rate limiter, and error handler are optional.
    /// </summary>
    public NaturalQueryEngine(
        ILlmProvider llmProvider,
        IQueryExecutor queryExecutor,
        IOptions<NaturalQueryOptions> options,
        ILogger<NaturalQueryEngine> logger,
        IQueryCache? cache = null,
        IRateLimiter? rateLimiter = null,
        IErrorHandler? errorHandler = null,
        IAuditSink? auditSink = null)
    {
        _llmProvider = llmProvider;
        _queryExecutor = queryExecutor;
        _options = options.Value;
        _logger = logger;
        _cache = cache;
        _rateLimiter = rateLimiter;
        _errorHandler = errorHandler;
        _auditSink = auditSink;
        _injectionScreener = new PromptInjectionScreener(_options.InjectionPatterns);
    }

    /// <inheritdoc />
    public async Task<QueryResult> AskAsync(string question, string? tenantId = null, ConversationContext? context = null, CancellationToken ct = default)
    {
        using var activity = NaturalQueryDiagnostics.StartAsk(question, tenantId);
        var stopwatch = Stopwatch.StartNew();
        var correlationId = GenerateCorrelationId();

        // Empty string is treated as "not provided" (consistent with null)
        if (tenantId is { Length: 0 })
            tenantId = null;

        // Audit tracking — one record per processed question, success or failure (FR-018)
        var auditOutcome = "llm_error";
        string? auditSql = null;
        var auditTokens = 0;
        var auditTruncated = false;
        Exception? failure = null;

        try
        {
            // Tenant identifier policy — untrusted input, validated before ANY use (FR-006)
            var tenantIdError = TenantIdValidator.Validate(tenantId, _options.TenantIdPattern);
            if (tenantIdError != null)
            {
                auditOutcome = "validation_rejected";
                var error = new NaturalQueryError
                {
                    Question = question,
                    ErrorType = "validation",
                    Message = tenantIdError,
                    TenantId = tenantId,
                    ElapsedMs = stopwatch.ElapsedMilliseconds
                };
                await ReportErrorAsync(error, ct);
                throw new InvalidOperationException($"Invalid tenant identifier: {tenantIdError}");
            }

            // Conversation-history screening — caller-supplied turns pass the same SQL
            // safety rules before inclusion in AI context, unconditionally (FR-013)
            if (context != null)
            {
                foreach (var turn in context.Turns)
                {
                    var turnError = SqlValidator.Validate(turn.Sql);
                    if (turnError != null)
                    {
                        auditOutcome = "validation_rejected";
                        var error = new NaturalQueryError
                        {
                            Question = question,
                            ErrorType = "validation",
                            Message = $"Conversation history contains a disallowed query: {turnError}",
                            TenantId = tenantId,
                            ElapsedMs = stopwatch.ElapsedMilliseconds
                        };
                        await ReportErrorAsync(error, ct);
                        throw new InvalidOperationException("Conversation history contains a disallowed query.");
                    }
                }
            }

            // Prompt-injection screening over question and history turns (FR-014)
            var injectionFlagged = false;
            if (_options.InjectionScreening != InjectionScreeningMode.Off)
            {
                var suspicious = _injectionScreener.IsSuspicious(question, out var matchedPattern);
                if (!suspicious && context != null)
                {
                    foreach (var turn in context.Turns)
                    {
                        if (_injectionScreener.IsSuspicious(turn.Question, out matchedPattern))
                        {
                            suspicious = true;
                            break;
                        }
                    }
                }

                if (suspicious)
                {
                    if (_options.InjectionScreening == InjectionScreeningMode.Block)
                    {
                        auditOutcome = "injection_flagged";
                        var error = new NaturalQueryError
                        {
                            Question = question,
                            ErrorType = "injection",
                            Message = $"Question flagged by prompt-injection screening (pattern: {matchedPattern}).",
                            TenantId = tenantId,
                            ElapsedMs = stopwatch.ElapsedMilliseconds
                        };
                        await ReportErrorAsync(error, ct);
                        throw new InvalidOperationException("Question was refused by prompt-injection screening.");
                    }

                    _logger.LogWarning(
                        "NaturalQuery prompt-injection screening flagged a question (pattern: {Pattern}). Proceeding in Warn mode.",
                        matchedPattern);
                    injectionFlagged = true;
                }
            }

            // Rate limiting
            if (_rateLimiter != null)
            {
                var allowed = await _rateLimiter.IsAllowedAsync(tenantId ?? "global", ct);
                if (!allowed)
                {
                    auditOutcome = "rate_limited";
                    var error = new NaturalQueryError
                    {
                        Question = question,
                        ErrorType = "rate_limit",
                        Message = "Rate limit exceeded.",
                        TenantId = tenantId,
                        ElapsedMs = stopwatch.ElapsedMilliseconds
                    };
                    await ReportErrorAsync(error, ct);
                    throw new InvalidOperationException("Rate limit exceeded. Try again later.");
                }
            }

            // Cache check
            if (_cache != null)
            {
                var cached = await _cache.GetAsync(question, tenantId, ct);
                if (cached != null)
                {
                    _logger.LogInformation("NaturalQuery cache hit for question: {Question}", question[..Math.Min(50, question.Length)]);
                    NaturalQueryDiagnostics.RecordCacheHit(activity);
                    cached.InjectionFlagged = injectionFlagged;
                    auditOutcome = "success";
                    auditSql = cached.Sql;
                    auditTokens = cached.TokensUsed;
                    auditTruncated = cached.Truncated;
                    return cached;
                }
            }

            // Interpret
            var result = await InterpretAsync(question, tenantId, context, ct);

            // Replace tenant placeholder
            var sql = result.Sql;
            if (!string.IsNullOrEmpty(_options.TenantIdPlaceholder) && !string.IsNullOrEmpty(tenantId))
                sql = sql.Replace(_options.TenantIdPlaceholder, tenantId);

            auditSql = sql;
            auditTokens = result.TokensUsed;

            // Validate SQL
            var validationError = ValidateGeneratedSql(sql, tenantId);
            if (validationError != null)
            {
                auditOutcome = "validation_rejected";
                var error = new NaturalQueryError
                {
                    Question = question,
                    Sql = sql,
                    ErrorType = "validation",
                    Message = validationError,
                    TenantId = tenantId,
                    TokensUsed = result.TokensUsed,
                    ElapsedMs = stopwatch.ElapsedMilliseconds
                };
                await ReportErrorAsync(error, ct);
                throw new InvalidOperationException($"Invalid query: {validationError}");
            }

            // Schema validation (if tables are configured)
            if (_options.Tables.Count > 0)
            {
                var schemaError = SchemaValidator.ValidateColumns(sql, _options.Tables);
                if (schemaError != null)
                    _logger.LogWarning("Schema validation warning: {Warning}", schemaError);
            }

            // Execute with optional retry
            auditOutcome = "execution_error";
            var maxRetries = Math.Clamp(_options.MaxRetries, 0, 3);
            Exception? lastExecutionError = null;

            for (var attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    if (attempt > 0)
                    {
                        _logger.LogWarning(
                            "NaturalQuery retry attempt {Attempt}/{MaxRetries} for question: {Question}",
                            attempt, maxRetries, question[..Math.Min(50, question.Length)]);

                        // Build repair prompt and send to LLM
                        var repairPrompt =
                            $"The following SQL query failed with this error: {lastExecutionError!.Message}. " +
                            $"Original question: {question}. " +
                            $"Failed SQL: {sql}. " +
                            $"Please fix the SQL and return the corrected JSON response.";

                        var systemPrompt = BuildSystemPrompt();
                        var repairResponse = await _llmProvider.GenerateAsync(systemPrompt, repairPrompt, ct);
                        result = ParseResponse(repairResponse);

                        // Replace tenant placeholder in the new SQL
                        sql = result.Sql;
                        if (!string.IsNullOrEmpty(_options.TenantIdPlaceholder) && !string.IsNullOrEmpty(tenantId))
                            sql = sql.Replace(_options.TenantIdPlaceholder, tenantId);

                        auditSql = sql;
                        auditTokens = result.TokensUsed;

                        // Validate the new SQL — identical safety rules on the repair path (FR-004)
                        var retryValidationError = ValidateGeneratedSql(sql, tenantId);
                        if (retryValidationError != null)
                        {
                            lastExecutionError = new InvalidOperationException($"Invalid query: {retryValidationError}");
                            continue;
                        }
                    }

                    using var execActivity = NaturalQueryDiagnostics.StartQueryExecution(sql);
                    await ExecuteWithGovernanceAsync(result, sql, ct);

                    // Execution succeeded, break out of retry loop
                    lastExecutionError = null;
                    break;
                }
                catch (Exception ex) when (attempt < maxRetries)
                {
                    lastExecutionError = ex;
                    _logger.LogWarning(ex, "NaturalQuery execution failed on attempt {Attempt}, will retry", attempt + 1);
                }
            }

            if (lastExecutionError != null)
                throw lastExecutionError;

            // Sensitive-column masking before caching/return (FR-019)
            if (_options.Tables.Count > 0)
                SensitiveDataMasker.Mask(result, _options.Tables);

            stopwatch.Stop();
            result.ElapsedMs = stopwatch.ElapsedMilliseconds;
            result.CorrelationId = correlationId;
            result.InjectionFlagged = injectionFlagged;
            auditOutcome = "success";
            auditTruncated = result.Truncated;

            NaturalQueryDiagnostics.RecordResult(activity, result.TokensUsed, result.ChartType, result.ElapsedMs);

            _logger.LogInformation("NaturalQuery completed in {ElapsedMs}ms. Chart: {ChartType}, Tokens: {Tokens}",
                result.ElapsedMs, result.ChartType, result.TokensUsed);

            // Cache store
            if (_cache != null)
                await _cache.SetAsync(question, tenantId, result, ct);

            // Add to conversation context
            context?.AddTurn(question, result.Sql);

            return result;
        }
        catch (Exception ex)
        {
            failure = ex;
            NaturalQueryDiagnostics.RecordError(activity, ex);

            if (_errorHandler != null && ex is not InvalidOperationException)
            {
                var error = new NaturalQueryError
                {
                    Question = question,
                    ErrorType = ClassifyError(ex),
                    Message = ex.Message,
                    TenantId = tenantId,
                    ElapsedMs = stopwatch.ElapsedMilliseconds,
                    Exception = ex
                };
                await ReportErrorAsync(error, ct);
            }

            throw;
        }
        finally
        {
            if (_auditSink != null)
            {
                if (failure is TimeoutException)
                    auditOutcome = "timeout";
                else if (auditOutcome == "llm_error" && failure is InvalidOperationException ioe && IsSafetyValidationMessage(ioe.Message))
                    auditOutcome = "validation_rejected"; // generated SQL rejected during parse-time validation

                await EmitAuditAsync(new AuditRecord
                {
                    Question = question,
                    Sql = auditSql,
                    TenantId = tenantId,
                    Outcome = auditOutcome,
                    DurationMs = stopwatch.ElapsedMilliseconds,
                    TokensUsed = auditTokens,
                    TimestampUtc = DateTime.UtcNow,
                    CorrelationId = correlationId,
                    Truncated = auditTruncated
                });
            }
        }
    }

    /// <summary>
    /// Writes the audit record; a sink failure never fails the user's request —
    /// it is logged server-side (FR-018). Uses CancellationToken.None so a
    /// caller-cancelled request still audits.
    /// </summary>
    private async Task EmitAuditAsync(AuditRecord record)
    {
        try
        {
            await _auditSink!.WriteAsync(record, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "NaturalQuery audit sink failed for correlation {CorrelationId}", record.CorrelationId);
        }
    }

    /// <inheritdoc />
    public async Task<QueryResult> InterpretAsync(string question, string? tenantId = null, ConversationContext? context = null, CancellationToken ct = default)
    {
        var systemPrompt = BuildSystemPrompt();

        // Build user prompt with conversation context
        var userPrompt = BuildUserPrompt(question, context);

        using var llmActivity = NaturalQueryDiagnostics.StartLlmGeneration("configured");
        var response = await _llmProvider.GenerateAsync(systemPrompt, userPrompt, ct);

        return ParseResponse(response);
    }

    /// <inheritdoc />
    public async Task<string> ExplainAsync(string sql, CancellationToken ct = default)
    {
        var prompt = $"Explain this SQL query in simple, plain language. Be concise (2-3 sentences max):\n\n{sql}";
        var response = await _llmProvider.GenerateAsync(
            "You are a SQL expert. Explain queries in simple language that non-technical people can understand. Be brief and clear.",
            prompt, ct);

        return response.Text.Trim();
    }

    /// <inheritdoc />
    public async Task<List<string>> SuggestQuestionsAsync(int count = 5, CancellationToken ct = default)
    {
        var schemaText = BuildSchemaText();
        var prompt = $"Given these database tables, suggest {count} useful analytical questions a business user might ask. Return ONLY a JSON array of strings, nothing else.\n\nTables:\n{schemaText}";

        var response = await _llmProvider.GenerateAsync(
            "You are a data analyst. Suggest practical, business-relevant questions. Return only a JSON array of strings.",
            prompt, ct);

        var text = response.Text.Trim();
        if (text.StartsWith("```"))
        {
            var firstNl = text.IndexOf('\n');
            if (firstNl > 0) text = text[(firstNl + 1)..];
            if (text.EndsWith("```")) text = text[..^3];
            text = text.Trim();
        }

        try
        {
            var suggestions = JsonSerializer.Deserialize<List<string>>(text);
            return suggestions?.Take(count).ToList() ?? new List<string>();
        }
        catch (JsonException)
        {
            _logger.LogWarning("Could not parse LLM suggestions response as JSON array");
            return new List<string>();
        }
    }

    /// <summary>
    /// Builds the system prompt from the configured options and table schemas.
    /// Public for testing and debugging purposes.
    /// </summary>
    public string BuildSystemPrompt()
    {
        if (!string.IsNullOrEmpty(_options.CustomSystemPrompt))
        {
            var schemaText = BuildSchemaText();
            return _options.CustomSystemPrompt.Replace("{TABLES_SCHEMA}", schemaText);
        }

        return GenerateDefaultPrompt();
    }

    /// <summary>
    /// Parses a raw LLM response into a structured QueryResult.
    /// Validates the SQL, extracts metadata, and handles edge cases.
    /// Public for testing and debugging purposes.
    /// </summary>
    public static QueryResult ParseResponse(LlmResponse response)
    {
        var text = response.Text.Trim();

        // Strip markdown fences
        if (text.StartsWith("```"))
        {
            var firstNewline = text.IndexOf('\n');
            if (firstNewline > 0) text = text[(firstNewline + 1)..];
            if (text.EndsWith("```")) text = text[..^3];
            text = text.Trim();
        }

        // Extract JSON object
        var start = text.IndexOf('{');
        var end = text.LastIndexOf('}');
        if (start < 0 || end < 0 || end <= start)
            throw new InvalidOperationException("Could not parse LLM response as JSON.");

        text = text[start..(end + 1)];

        JsonElement root;
        try
        {
            root = JsonSerializer.Deserialize<JsonElement>(text);
        }
        catch (JsonException)
        {
            throw new InvalidOperationException("Could not parse LLM response as JSON.");
        }

        // Check for error response
        if (root.TryGetProperty("error", out var errProp) && errProp.GetBoolean())
        {
            var msg = root.TryGetProperty("message", out var msgProp) ? msgProp.GetString() : "Unknown error";
            throw new InvalidOperationException(msg);
        }

        // Extract SQL (required)
        var sql = root.TryGetProperty("sql", out var sqlProp) ? sqlProp.GetString() ?? "" : "";
        if (string.IsNullOrWhiteSpace(sql))
            throw new InvalidOperationException("LLM response is missing the 'sql' field.");

        // Validate SQL safety with the same hardened pipeline used everywhere else (FR-004)
        var safetyError = Validation.SqlValidator.Validate(sql);
        if (safetyError != null)
            throw new InvalidOperationException(safetyError);

        // Chart type
        var chartType = root.TryGetProperty("chartType", out var ctProp) ? ctProp.GetString() ?? "table" : "table";
        if (!ChartType.IsValid(chartType)) chartType = "table";

        // Title & description
        var title = root.TryGetProperty("title", out var titleProp) ? titleProp.GetString() ?? "" : "";
        var description = root.TryGetProperty("description", out var descProp) ? descProp.GetString() ?? "" : "";

        // Suggestions
        var suggestions = new List<string>();
        if (root.TryGetProperty("suggestions", out var sugProp) && sugProp.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in sugProp.EnumerateArray())
            {
                var s = item.GetString();
                if (!string.IsNullOrWhiteSpace(s) && suggestions.Count < 3)
                    suggestions.Add(s);
            }
        }

        return new QueryResult
        {
            Sql = sql,
            ChartType = chartType,
            Title = title,
            Description = description,
            Suggestions = suggestions,
            TokensUsed = response.TokensUsed
        };
    }

    private string GenerateDefaultPrompt()
    {
        var sb = new StringBuilder();

        sb.AppendLine("You are a SQL report generator. Interpret natural language questions and return a JSON object with a valid SQL query.");
        sb.AppendLine();
        sb.AppendLine("=== AVAILABLE TABLES ===");
        sb.AppendLine();
        sb.Append(BuildSchemaText());
        sb.AppendLine();

        sb.AppendLine("=== CHART TYPES ===");
        sb.AppendLine("- \"line\": time series (SELECT period AS label, COUNT(*) AS value)");
        sb.AppendLine("- \"bar\": category comparisons (SELECT field AS label, COUNT(*) AS value)");
        sb.AppendLine("- \"bar_horizontal\": rankings (SELECT field AS label, COUNT(*) AS value ORDER BY value DESC)");
        sb.AppendLine("- \"pie\" / \"donut\": distributions (SELECT field AS label, COUNT(*) AS value)");
        sb.AppendLine("- \"metric\": single number (SELECT COUNT(*) AS value)");
        sb.AppendLine("- \"area\": filled time series");
        sb.AppendLine("- \"table\": detailed data list (SELECT col1, col2, ... — no label/value)");
        sb.AppendLine();

        sb.AppendLine("=== SQL RULES ===");

        if (!string.IsNullOrEmpty(_options.TenantIdColumn) && !string.IsNullOrEmpty(_options.TenantIdPlaceholder))
        {
            sb.AppendLine($"1. ALWAYS include WHERE {_options.TenantIdColumn} = '{_options.TenantIdPlaceholder}' for tenant isolation.");
        }

        sb.AppendLine("2. DEDUPLICATION: If multiple records exist per entity (INSERT, MODIFY events), use ROW_NUMBER() OVER (PARTITION BY id ORDER BY _eventtime DESC) to get the latest state. Exclude _eventname = 'REMOVE'.");
        sb.AppendLine("3. For charts: return EXACTLY 2 columns: \"label\" and \"value\".");
        sb.AppendLine("4. For tables: use descriptive column aliases via AS.");
        sb.AppendLine("5. Use LIMIT to avoid returning too many rows (default: 100 for tables, 50 for rankings).");
        sb.AppendLine("6. NEVER use DELETE, UPDATE, INSERT, DROP, ALTER, CREATE. Only SELECT.");
        sb.AppendLine();

        if (_options.AdditionalRules.Count > 0)
        {
            sb.AppendLine("=== ADDITIONAL RULES ===");
            foreach (var rule in _options.AdditionalRules)
                sb.AppendLine($"- {rule}");
            sb.AppendLine();
        }

        sb.AppendLine("=== RESPONSE FORMAT ===");
        sb.AppendLine("{\"sql\":\"SELECT ...\",\"chartType\":\"table\",\"title\":\"...\",\"description\":\"...\",\"suggestions\":[\"...\"]}");
        sb.AppendLine();

        sb.AppendLine("=== GUARDRAILS ===");
        sb.AppendLine("1. Return ONLY pure JSON. No markdown, no extra text.");
        sb.AppendLine("2. The \"sql\" field is REQUIRED in every response.");
        sb.AppendLine("3. Title and description should be concise.");
        sb.AppendLine("4. Suggestions: up to 3 follow-up report ideas.");

        return sb.ToString();
    }

    private string BuildSchemaText()
    {
        var sb = new StringBuilder();
        foreach (var table in _options.Tables)
        {
            var desc = string.IsNullOrEmpty(table.Description) ? "" : $" — {table.Description}";
            var partitions = table.Partitions.Count > 0
                ? $" (partitions: {string.Join(", ", table.Partitions)})"
                : "";

            sb.AppendLine($"{table.Name}{partitions}{desc}");
            sb.Append("  Columns: ");
            sb.AppendJoin(", ", table.Columns.Select(c =>
            {
                var colDesc = string.IsNullOrEmpty(c.Description) ? "" : $" ({c.Description})";
                return $"{c.Name} ({c.Type}){colDesc}";
            }));
            sb.AppendLine();
            sb.AppendLine();
        }
        return sb.ToString();
    }

    private static string BuildUserPrompt(string question, ConversationContext? context)
    {
        if (context == null || context.Turns.Count == 0)
            return question;

        var sb = new StringBuilder();
        sb.AppendLine("Previous conversation:");
        foreach (var turn in context.Turns)
        {
            sb.AppendLine($"- User asked: \"{turn.Question}\"");
            sb.AppendLine($"  Generated SQL: {turn.Sql}");
        }
        sb.AppendLine();
        sb.AppendLine($"New question (follow-up): {question}");
        return sb.ToString();
    }

    private async Task ReportErrorAsync(NaturalQueryError error, CancellationToken ct)
    {
        if (_errorHandler == null) return;
        try
        {
            await _errorHandler.HandleAsync(error, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error handler failed");
        }
    }

    /// <summary>
    /// Executes the query under resource governance: a linked cancellation token
    /// enforces QueryTimeoutSeconds (FR-017), and results are truncated at
    /// MaxResultRows with the Truncated marker set (FR-016). The marker is only set
    /// when the cap was the binding constraint — a query whose own LIMIT produced
    /// fewer (or exactly cap) rows is not marked.
    /// </summary>
    private async Task ExecuteWithGovernanceAsync(QueryResult result, string sql, CancellationToken ct)
    {
        var timeoutSeconds = _options.QueryTimeoutSeconds;
        using var timeoutCts = timeoutSeconds > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(ct)
            : null;
        timeoutCts?.CancelAfter(TimeSpan.FromSeconds(timeoutSeconds));
        var execToken = timeoutCts?.Token ?? ct;

        try
        {
            if (result.ChartType == ChartType.Table)
            {
                result.TableData = await _queryExecutor.ExecuteTableQueryAsync(sql, execToken);
            }
            else
            {
                result.ChartData = await _queryExecutor.ExecuteChartQueryAsync(sql, execToken);
            }
        }
        catch (OperationCanceledException) when (timeoutCts is { IsCancellationRequested: true } && !ct.IsCancellationRequested)
        {
            throw new TimeoutException($"Query execution exceeded the {timeoutSeconds}s timeout.");
        }

        var cap = _options.MaxResultRows;
        if (cap > 0)
        {
            if (result.TableData is { } table && table.Count > cap)
            {
                result.TableData = table.Take(cap).ToList();
                result.Truncated = true;
            }

            if (result.ChartData is { } chart && chart.Count > cap)
            {
                result.ChartData = chart.Take(cap).ToList();
                result.Truncated = true;
            }
        }
    }

    /// <summary>
    /// Applies every SQL safety rule used at any route to execution: hardened keyword
    /// validation plus structural tenant-filter verification (FR-004, FR-007).
    /// </summary>
    private string? ValidateGeneratedSql(string sql, string? tenantId)
    {
        var validationError = SqlValidator.Validate(sql, _options.TenantIdColumn, tenantId, _options.ForbiddenSqlKeywords);
        if (validationError != null)
            return validationError;

        // Structural tenant-filter verification — no-op when tenant column unconfigured (FR-008)
        if (!string.IsNullOrEmpty(_options.TenantIdColumn) && !string.IsNullOrEmpty(tenantId) &&
            !TenantFilterVerifier.HasTenantFilter(sql, _options.TenantIdColumn, tenantId))
        {
            return $"Query must apply an equality filter on tenant column '{_options.TenantIdColumn}'.";
        }

        return null;
    }

    /// <summary>Matches the error strings owned by SqlValidator (safety rejections).</summary>
    private static bool IsSafetyValidationMessage(string message) =>
        message.Contains("Forbidden SQL keyword", StringComparison.Ordinal) ||
        message.Contains("Only SELECT queries", StringComparison.Ordinal) ||
        message.Contains("Multiple SQL statements", StringComparison.Ordinal) ||
        message.Contains("SQL query cannot be empty", StringComparison.Ordinal);

    private static string GenerateCorrelationId()
    {
        var traceId = Activity.Current?.TraceId.ToString();
        return string.IsNullOrEmpty(traceId) || traceId == "00000000000000000000000000000000"
            ? Guid.NewGuid().ToString("N")
            : traceId;
    }

    private static string ClassifyError(Exception ex) => ex switch
    {
        TimeoutException => "timeout",
        InvalidOperationException ioe when ioe.Message.Contains("rate limit", StringComparison.OrdinalIgnoreCase) => "rate_limit",
        InvalidOperationException ioe when ioe.Message.Contains("JSON", StringComparison.OrdinalIgnoreCase) => "parsing",
        InvalidOperationException ioe when ioe.Message.Contains("Query failed", StringComparison.OrdinalIgnoreCase) => "execution",
        InvalidOperationException => "validation",
        _ => "unknown"
    };
}

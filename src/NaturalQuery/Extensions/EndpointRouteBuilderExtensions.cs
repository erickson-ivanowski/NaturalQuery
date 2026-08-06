using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NaturalQuery.Models;

namespace NaturalQuery.Extensions;

/// <summary>
/// Extension methods for mapping NaturalQuery endpoints in ASP.NET minimal APIs.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps NaturalQuery endpoints at the specified path prefix.
    /// Creates two endpoints:
    /// GET {prefix}?q=...&amp;tenantId=... for simple queries
    /// POST {prefix} with JSON body for full features (conversation context, etc.)
    /// </summary>
    public static IEndpointRouteBuilder MapNaturalQuery(this IEndpointRouteBuilder endpoints, string prefix = "/ask")
        => MapNaturalQueryCore(endpoints, prefix, null);

    /// <summary>
    /// Maps NaturalQuery endpoints with opt-in protections: input size limits,
    /// safe error responses carrying a correlation ID (full detail only in server
    /// logs), and host-delegated authorization.
    /// </summary>
    public static IEndpointRouteBuilder MapNaturalQuery(
        this IEndpointRouteBuilder endpoints,
        string prefix,
        NaturalQueryEndpointOptions options)
        => MapNaturalQueryCore(endpoints, prefix, options ?? new NaturalQueryEndpointOptions());

    private static IEndpointRouteBuilder MapNaturalQueryCore(
        IEndpointRouteBuilder endpoints,
        string prefix,
        NaturalQueryEndpointOptions? options)
    {
        // Normalize prefix
        prefix = prefix.TrimEnd('/');

        // GET endpoint for simple queries
        var get = endpoints.MapGet(prefix, async (HttpContext context) =>
        {
            var engine = context.RequestServices.GetRequiredService<INaturalQueryEngine>();
            var question = context.Request.Query["q"].ToString();
            var tenantId = context.Request.Query["tenantId"].ToString();

            if (string.IsNullOrWhiteSpace(question))
                return Results.BadRequest(new { error = "Query parameter 'q' is required." });

            var limitError = CheckLimits(context, options, question, contextTurnCount: 0);
            if (limitError != null)
                return limitError;

            var (page, pageSize) = ReadPagingParams(context.Request.Query["page"], context.Request.Query["pageSize"]);

            try
            {
                var normalizedTenantId = string.IsNullOrEmpty(tenantId) ? null : tenantId;
                var result = page.HasValue
                    ? await engine.AskPagedAsync(question, page.Value, pageSize ?? DefaultPageSize, normalizedTenantId, ct: context.RequestAborted)
                    : await engine.AskAsync(question, normalizedTenantId, ct: context.RequestAborted);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return HandleKnownError(context, options, ex);
            }
            catch (Exception ex) when (options != null)
            {
                return HandleUnexpectedError(context, ex);
            }
        })
        .WithName("NaturalQuery_Get")
        .WithTags("NaturalQuery");

        // POST endpoint for full features
        var post = endpoints.MapPost(prefix, async (HttpContext context) =>
        {
            var engine = context.RequestServices.GetRequiredService<INaturalQueryEngine>();

            NaturalQueryRequest? request;
            try
            {
                request = await context.Request.ReadFromJsonAsync<NaturalQueryRequest>(context.RequestAborted);
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid JSON body." });
            }

            if (request == null || string.IsNullOrWhiteSpace(request.Question))
                return Results.BadRequest(new { error = "Field 'question' is required." });

            var limitError = CheckLimits(context, options, request.Question, request.Context?.Count ?? 0);
            if (limitError != null)
                return limitError;

            // Build conversation context if provided
            ConversationContext? conversationContext = null;
            if (request.Context?.Count > 0)
            {
                conversationContext = new ConversationContext();
                foreach (var turn in request.Context)
                    conversationContext.AddTurn(turn.Question, turn.Sql);
            }

            try
            {
                var result = request.Page.HasValue
                    ? await engine.AskPagedAsync(request.Question, request.Page.Value, request.PageSize ?? DefaultPageSize, request.TenantId, conversationContext, context.RequestAborted)
                    : await engine.AskAsync(request.Question, request.TenantId, conversationContext, context.RequestAborted);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return HandleKnownError(context, options, ex);
            }
            catch (Exception ex) when (options != null)
            {
                return HandleUnexpectedError(context, ex);
            }
        })
        .WithName("NaturalQuery_Post")
        .WithTags("NaturalQuery");

        // POST {prefix}/preview — generated query + estimated cost, no execution (FR-032)
        var preview = endpoints.MapPost($"{prefix}/preview", async (HttpContext context) =>
        {
            var engine = context.RequestServices.GetRequiredService<INaturalQueryEngine>();

            NaturalQueryRequest? request;
            try
            {
                request = await context.Request.ReadFromJsonAsync<NaturalQueryRequest>(context.RequestAborted);
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid JSON body." });
            }

            if (request == null || string.IsNullOrWhiteSpace(request.Question))
                return Results.BadRequest(new { error = "Field 'question' is required." });

            var limitError = CheckLimits(context, options, request.Question, request.Context?.Count ?? 0);
            if (limitError != null)
                return limitError;

            ConversationContext? conversationContext = null;
            if (request.Context?.Count > 0)
            {
                conversationContext = new ConversationContext();
                foreach (var turn in request.Context)
                    conversationContext.AddTurn(turn.Question, turn.Sql);
            }

            try
            {
                var preview = await engine.PreviewAsync(request.Question, request.TenantId, conversationContext, context.RequestAborted);
                return Results.Ok(preview);
            }
            catch (InvalidOperationException ex)
            {
                return HandleKnownError(context, options, ex);
            }
            catch (Exception ex) when (options != null)
            {
                return HandleUnexpectedError(context, ex);
            }
        })
        .WithName("NaturalQuery_Preview")
        .WithTags("NaturalQuery");

        // POST {prefix}/execute — re-validates and runs a previously previewed query (FR-032)
        var execute = endpoints.MapPost($"{prefix}/execute", async (HttpContext context) =>
        {
            var engine = context.RequestServices.GetRequiredService<INaturalQueryEngine>();

            NaturalQueryExecuteRequest? request;
            try
            {
                request = await context.Request.ReadFromJsonAsync<NaturalQueryExecuteRequest>(context.RequestAborted);
            }
            catch
            {
                return Results.BadRequest(new { error = "Invalid JSON body." });
            }

            if (request == null || string.IsNullOrWhiteSpace(request.Sql))
                return Results.BadRequest(new { error = "Field 'sql' is required." });

            try
            {
                var result = await engine.ExecuteApprovedAsync(request.Sql, request.TenantId, context.RequestAborted);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return HandleKnownError(context, options, ex);
            }
            catch (Exception ex) when (options != null)
            {
                return HandleUnexpectedError(context, ex);
            }
        })
        .WithName("NaturalQuery_Execute")
        .WithTags("NaturalQuery");

        if (options is { } o && (o.RequireAuthorization || !string.IsNullOrEmpty(o.AuthorizationPolicy)))
        {
            if (!string.IsNullOrEmpty(o.AuthorizationPolicy))
            {
                get.RequireAuthorization(o.AuthorizationPolicy);
                post.RequireAuthorization(o.AuthorizationPolicy);
                preview.RequireAuthorization(o.AuthorizationPolicy);
                execute.RequireAuthorization(o.AuthorizationPolicy);
            }
            else
            {
                get.RequireAuthorization();
                post.RequireAuthorization();
                preview.RequireAuthorization();
                execute.RequireAuthorization();
            }
        }

        return endpoints;
    }

    private const int DefaultPageSize = 100;

    private static (int? Page, int? PageSize) ReadPagingParams(
        Microsoft.Extensions.Primitives.StringValues pageRaw,
        Microsoft.Extensions.Primitives.StringValues pageSizeRaw)
    {
        int? page = int.TryParse(pageRaw, out var p) ? p : null;
        int? pageSize = int.TryParse(pageSizeRaw, out var ps) ? ps : null;
        return (page, pageSize);
    }

    /// <summary>
    /// Enforces question-length and history-size caps before any engine (AI) work.
    /// Falls back to the engine's configured NaturalQueryOptions limits when the
    /// endpoint options leave them unset.
    /// </summary>
    private static IResult? CheckLimits(
        HttpContext context,
        NaturalQueryEndpointOptions? options,
        string question,
        int contextTurnCount)
    {
        var engineOptions = context.RequestServices
            .GetRequiredService<IOptions<NaturalQueryOptions>>().Value;

        var maxQuestion = options?.MaxQuestionLength ?? engineOptions.MaxQuestionLength;
        var maxTurns = options?.MaxContextTurns ?? engineOptions.MaxContextTurns;

        if (maxQuestion > 0 && question.Length > maxQuestion)
        {
            return Results.Json(
                new { error = $"Question exceeds the maximum length of {maxQuestion} characters.", correlationId = NewCorrelationId() },
                statusCode: StatusCodes.Status413PayloadTooLarge);
        }

        if (maxTurns > 0 && contextTurnCount > maxTurns)
        {
            return Results.Json(
                new { error = $"Conversation history exceeds the maximum of {maxTurns} turns.", correlationId = NewCorrelationId() },
                statusCode: StatusCodes.Status413PayloadTooLarge);
        }

        return null;
    }

    /// <summary>
    /// InvalidOperationException carries intentional, safe engine feedback
    /// (validation results, rate-limit notices). Legacy mapping preserves the exact
    /// historical shape; protected mapping adds a correlation ID and server-side log.
    /// </summary>
    private static IResult HandleKnownError(
        HttpContext context,
        NaturalQueryEndpointOptions? options,
        InvalidOperationException ex)
    {
        if (options == null)
            return Results.BadRequest(new { error = ex.Message });

        var correlationId = NewCorrelationId();
        Logger(context).LogWarning(ex, "NaturalQuery request rejected [{CorrelationId}]: {Message}", correlationId, ex.Message);
        return Results.BadRequest(new { error = ex.Message, correlationId });
    }

    /// <summary>
    /// Unexpected failures (AI provider, database, bugs) never leak detail to the
    /// caller: generic message + correlation ID, full exception in server logs.
    /// </summary>
    private static IResult HandleUnexpectedError(HttpContext context, Exception ex)
    {
        var correlationId = NewCorrelationId();
        Logger(context).LogError(ex, "NaturalQuery request failed [{CorrelationId}]", correlationId);
        return Results.Json(
            new { error = "An internal error occurred while processing the request.", correlationId },
            statusCode: StatusCodes.Status500InternalServerError);
    }

    private static ILogger Logger(HttpContext context) =>
        context.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("NaturalQuery.Endpoints");

    private static string NewCorrelationId() =>
        System.Diagnostics.Activity.Current?.TraceId.ToString() is { Length: > 0 } traceId
        && traceId != "00000000000000000000000000000000"
            ? traceId
            : Guid.NewGuid().ToString("N");
}

/// <summary>
/// Request body for the POST endpoint.
/// </summary>
public class NaturalQueryRequest
{
    /// <summary>Natural language question.</summary>
    public string Question { get; set; } = string.Empty;

    /// <summary>Optional tenant ID for multi-tenant isolation.</summary>
    public string? TenantId { get; set; }

    /// <summary>Optional conversation history for follow-up questions.</summary>
    public List<NaturalQueryContextTurn>? Context { get; set; }

    /// <summary>Optional 1-based page number; when set, results are paged (FR-031).</summary>
    public int? Page { get; set; }

    /// <summary>Optional rows per page (used only when Page is set).</summary>
    public int? PageSize { get; set; }
}

/// <summary>
/// Request body for the POST {prefix}/execute endpoint.
/// </summary>
public class NaturalQueryExecuteRequest
{
    /// <summary>The previously previewed SQL query to execute.</summary>
    public string Sql { get; set; } = string.Empty;

    /// <summary>Optional tenant ID for multi-tenant isolation.</summary>
    public string? TenantId { get; set; }
}

/// <summary>
/// A single conversation turn in the request context.
/// </summary>
public class NaturalQueryContextTurn
{
    /// <summary>The question that was asked.</summary>
    public string Question { get; set; } = string.Empty;

    /// <summary>The SQL that was generated.</summary>
    public string Sql { get; set; } = string.Empty;
}

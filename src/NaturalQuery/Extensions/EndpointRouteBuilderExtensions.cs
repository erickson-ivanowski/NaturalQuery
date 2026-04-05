using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
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
    {
        // Normalize prefix
        prefix = prefix.TrimEnd('/');

        // GET endpoint for simple queries
        endpoints.MapGet(prefix, async (HttpContext context) =>
        {
            var engine = context.RequestServices.GetRequiredService<INaturalQueryEngine>();
            var question = context.Request.Query["q"].ToString();
            var tenantId = context.Request.Query["tenantId"].ToString();

            if (string.IsNullOrWhiteSpace(question))
                return Results.BadRequest(new { error = "Query parameter 'q' is required." });

            try
            {
                var result = await engine.AskAsync(question,
                    string.IsNullOrEmpty(tenantId) ? null : tenantId,
                    ct: context.RequestAborted);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        })
        .WithName("NaturalQuery_Get")
        .WithTags("NaturalQuery");

        // POST endpoint for full features
        endpoints.MapPost(prefix, async (HttpContext context) =>
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
                var result = await engine.AskAsync(
                    request.Question,
                    request.TenantId,
                    conversationContext,
                    context.RequestAborted);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        })
        .WithName("NaturalQuery_Post")
        .WithTags("NaturalQuery");

        return endpoints;
    }
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

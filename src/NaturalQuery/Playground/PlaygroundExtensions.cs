using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NaturalQuery.Playground;

/// <summary>
/// Extension methods for mapping the NaturalQuery Playground UI.
/// The playground is a development-only tool (like Swagger UI) for testing
/// natural language queries interactively in the browser.
/// </summary>
public static class PlaygroundExtensions
{
    /// <summary>
    /// Maps the NaturalQuery Playground UI at the specified path.
    /// This serves an interactive HTML page where developers can test queries.
    /// Should only be used in development environments.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="path">URL path for the playground. Default: "/nq-playground".</param>
    /// <param name="apiPath">The NaturalQuery API endpoint path. Default: "/ask".</param>
    /// <param name="allowInProduction">
    /// When false (default), the playground refuses to serve (404) outside the
    /// Development environment and logs a warning explaining why (FR-012).
    /// </param>
    public static IEndpointRouteBuilder MapNaturalQueryPlayground(
        this IEndpointRouteBuilder endpoints,
        string path = "/nq-playground",
        string apiPath = "/ask",
        bool allowInProduction = false)
    {
        endpoints.MapGet(path, (HttpContext context) =>
        {
            // Production guard: development-only unless explicitly opted in
            var environment = context.RequestServices.GetService<IHostEnvironment>();
            if (!allowInProduction && environment != null && !environment.IsDevelopment())
            {
                context.RequestServices.GetRequiredService<ILoggerFactory>()
                    .CreateLogger("NaturalQuery.Playground")
                    .LogWarning(
                        "NaturalQuery playground refused to serve in environment '{Environment}'. " +
                        "It is development-only; pass allowInProduction: true to MapNaturalQueryPlayground to opt in.",
                        environment.EnvironmentName);
                return Results.NotFound();
            }

            var assembly = typeof(PlaygroundExtensions).Assembly;
            var resourceName = "NaturalQuery.Playground.playground.html";

            using var stream = assembly.GetManifestResourceStream(resourceName);
            if (stream == null)
                return Results.NotFound("Playground resource not found.");

            using var reader = new System.IO.StreamReader(stream);
            var html = reader.ReadToEnd();

            // Inject the API path
            html = html.Replace("{{API_PATH}}", apiPath);

            return Results.Content(html, "text/html");
        })
        .ExcludeFromDescription(); // Hide from OpenAPI docs

        return endpoints;
    }
}

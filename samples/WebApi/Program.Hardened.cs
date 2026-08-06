// Hardened-configuration example (SC-007): enables authorization, limits, audit,
// and masking in fewer than 10 lines of configuration. This file is not wired
// into the running sample app — it demonstrates the opt-in security surface
// without touching the existing Program.cs code path.
//
// To try it: rename Program.cs, rename this file to Program.cs, run `dotnet run`.
using NaturalQuery;
using NaturalQuery.Extensions;
using NaturalQuery.Models;
using NaturalQuery.Security;

var builder = WebApplication.CreateBuilder(args);

var apiKey = builder.Configuration["OpenAI:ApiKey"]
    ?? Environment.GetEnvironmentVariable("OPENAI_API_KEY")
    ?? throw new InvalidOperationException("Set OpenAI:ApiKey in config or OPENAI_API_KEY env var.");

builder.Services.AddAuthentication().AddCookie();
builder.Services.AddAuthorization();

builder.Services.AddNaturalQuery(options =>
{
    options.Tables = new List<TableSchema>
    {
        new("customers", new[]
        {
            new ColumnDef("id", "int"),
            new ColumnDef("name", "string"),
            new ColumnDef("email", "string", "customer email", sensitive: true), // masked as *** in every output
        })
    };
    options.TenantIdColumn = "tenant_id";
    options.TenantIdPlaceholder = "{TENANT_ID}";
    options.InjectionScreening = InjectionScreeningMode.Block; // strict mode
})
.UseOpenAiProvider(apiKey)
.UseSqliteExecutor("DataSource=sample.db")
.UseInMemoryCache()
.UseInMemoryRateLimiter()
.UseAuditSink((record, ct) =>
{
    Console.WriteLine($"[audit] {record.TimestampUtc:O} outcome={record.Outcome} tenant={record.TenantId}");
    return Task.CompletedTask;
});

var app = builder.Build();

app.MapNaturalQuery("/ask", new NaturalQueryEndpointOptions { RequireAuthorization = true });

app.Run();

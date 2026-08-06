# NaturalQuery

[![NuGet](https://img.shields.io/nuget/v/NaturalQuery.svg)](https://www.nuget.org/packages/NaturalQuery)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

NL2SQL engine for .NET — convert natural language questions into SQL queries using LLMs.

```
"top 10 products by sales"  →  SELECT product AS label, SUM(amount) AS value FROM orders GROUP BY product ORDER BY value DESC LIMIT 10
```

## What it does

NaturalQuery takes a natural language question, sends it to an LLM with your database schema, and returns:
- A validated SQL query
- A recommended chart type (bar, pie, line, table, etc.)
- Title, description, and follow-up suggestions
- Executed results ready for visualization

## Quick Start

```bash
dotnet add package NaturalQuery
```

### With OpenAI + SQLite

```csharp
services.AddNaturalQuery(options =>
{
    options.Tables = new List<TableSchema>
    {
        new("products", new[]
        {
            new ColumnDef("id", "int"),
            new ColumnDef("name", "string", "product name"),
            new ColumnDef("category", "string"),
            new ColumnDef("price", "double", "USD"),
            new ColumnDef("in_stock", "int", "1=yes, 0=no"),
        })
    };
})
.UseOpenAiProvider("sk-your-api-key", model: "gpt-4o-mini")
.UseSqliteExecutor("DataSource=mydb.db");
```

### With AWS Bedrock + Athena

```csharp
builder.Services.AddNaturalQuery(options =>
{
    options.Tables = new List<TableSchema>
    {
        new("orders", new[]
        {
            new ColumnDef("id", "string"),
            new ColumnDef("product", "string"),
            new ColumnDef("amount", "double"),
            new ColumnDef("status", "string", "Active, Cancelled, Refunded"),
            new ColumnDef("created_at", "string", "ISO 8601 timestamp"),
        })
    };
    options.TenantIdColumn = "tenant_id";
    options.TenantIdPlaceholder = "{TENANT_ID}";
})
.UseBedrockProvider("us.anthropic.claude-haiku-4-5-20251001-v1:0")
.UseAthenaExecutor("my_database", "my_workgroup", "s3://my-results/");
```

### Usage

```csharp
var result = await engine.AskAsync("top products by revenue", tenantId: "my-tenant");

Console.WriteLine(result.Sql);       // SELECT product AS label, SUM(amount) ...
Console.WriteLine(result.ChartType); // bar
Console.WriteLine(result.Title);     // Top Products by Revenue
```

## Security & Hardening (v2.1)

NaturalQuery 2.1 hardens the pipeline end-to-end and adds enterprise features — all additive, zero breaking changes. See [CHANGELOG.md](CHANGELOG.md) for the full list and [specs/001-security-audit-enhancements/quickstart.md](specs/001-security-audit-enhancements/quickstart.md) for a full hardened-configuration example.

Enabled by default, no config needed:
- Obfuscation-resistant SQL validation (comments, whitespace tricks, dialect-specific write/execute operations all rejected)
- Tenant identifiers validated against a safe character policy; tenant filters structurally verified (not just text-matched)
- Endpoint input limits (2,000-char questions, 20-turn history), safe error responses with a correlation ID
- Bounded rate-limiter memory, capped result rows (`result.Truncated`), query execution timeout
- Prompt-injection screening (warn + flag by default; opt into strict blocking)
- Startup configuration validation — impossible configs fail fast with the exact reason

Opt-in:
- Audit trail (`UseAuditSink`) and sensitive-column masking (`ColumnDef(..., sensitive: true)`)
- MySQL/MariaDB, direct Anthropic provider, OpenRouter convenience
- Shared-store cache/rate limiter via the `NaturalQuery.Redis` companion package
- Health checks, native metrics, Excel export
- Semantic cache, pagination, preview/approve execution flow

## ASP.NET Integration

Map NaturalQuery as a web endpoint with a single line. This creates both GET and POST endpoints with conversation context support.

```csharp
var app = builder.Build();

app.MapNaturalQuery("/ask");
```

The GET endpoint accepts `?q=...&tenantId=...` for simple queries. The POST endpoint accepts a JSON body with `question`, `tenantId`, and an optional `context` array for follow-up conversations.

### Playground

NaturalQuery ships with a built-in playground UI for testing queries interactively in the browser. Think of it as Swagger UI for your NL2SQL endpoint.

```csharp
if (app.Environment.IsDevelopment())
{
    app.MapNaturalQueryPlayground("/playground", apiPath: "/ask");
}
```

Navigate to `/playground` and start typing questions. The playground shows the generated SQL, chart type, data, and response metadata in real time.

## Architecture

```
Question (natural language)
    ↓
[Cache check] → hit? return cached result
    ↓
[Rate limiter] → exceeded? throw
    ↓
ILlmProvider (Bedrock / OpenAI / custom)
    ↓
SQL generation + validation + schema check
    ↓
[Auto-retry on failure] → rephrase + retry (up to MaxRetries)
    ↓
IQueryExecutor (Athena / PostgreSQL / SQL Server / SQLite / CSV / custom)
    ↓
QueryResult (data + chart type + metadata)
    ↓
[Cache store] + [Diagnostics]
```

## Providers

| Provider | Type | Package/Dependency |
|----------|------|--------------------|
| AWS Bedrock (Claude) | LLM | AWSSDK.BedrockRuntime |
| OpenAI / compatible | LLM | None (raw HttpClient) |
| Amazon Athena | Query Executor | AWSSDK.Athena |
| PostgreSQL | Query Executor | Npgsql |
| SQL Server | Query Executor | Microsoft.Data.SqlClient |
| SQLite | Query Executor | Microsoft.Data.Sqlite |
| CSV | Query Executor | Built-in (loads into SQLite) |

All providers are included in the NaturalQuery package. Pick one LLM provider and one query executor.

### SQL Server

```csharp
.UseSqlServerExecutor("Server=localhost;Database=mydb;Trusted_Connection=true;", wrapInTransaction: true)
```

### SQLite

```csharp
.UseSqliteExecutor("DataSource=mydb.db")
```

### CSV Data Source

Load a CSV file (or stream) into an in-memory SQLite database and query it with natural language. Great for ad-hoc analytics on uploaded files.

```csharp
.UseCsvSource("sales-report.csv", tableName: "sales")

// Or from a stream (e.g., file upload)
.UseCsvSource(uploadedFileStream, tableName: "data")
```

## Features

### Auto-Retry with Rephrasing

When a generated query fails to execute, NaturalQuery can send the error back to the LLM and ask it to fix the SQL. This handles edge cases like dialect-specific syntax without manual intervention.

```csharp
options.MaxRetries = 2;  // default: 0 (no retries), max: 3
```

### Query Cache

Avoid redundant LLM calls for repeated questions. Cache key is a SHA256 hash of `(question + tenantId)` for privacy.

```csharp
.UseInMemoryCache()

options.CacheTtlMinutes = 10;  // default: 5
```

Implement `IQueryCache` for Redis or distributed caching.

### Rate Limiting

Per-tenant sliding window rate limiter.

```csharp
.UseInMemoryRateLimiter()

options.RateLimitPerMinute = 30;  // default: 60
```

Implement `IRateLimiter` for distributed rate limiting.

### Conversation Context (Follow-up Questions)

Support "now filter by cancelled ones" style follow-up questions.

```csharp
var context = new ConversationContext();

var result1 = await engine.AskAsync("all orders this month", tenantId: "t1", context: context);
// context now contains the first question + SQL

var result2 = await engine.AskAsync("now only cancelled ones", tenantId: "t1", context: context);
// LLM receives conversation history and generates a filtered query
```

### Query Explanation

Let users understand what the generated SQL does.

```csharp
var result = await engine.InterpretAsync("top products by revenue");
var explanation = await engine.ExplainAsync(result.Sql);
// "This query groups products by name and sums their revenue, returning the top results."
```

### Suggested Questions

Help users discover what they can ask.

```csharp
var suggestions = await engine.SuggestQuestionsAsync(count: 5);
// ["What are the top 10 products by sales?", "How many orders per month?", ...]
```

### Export Results

Export query results to CSV or JSON for downstream processing.

```csharp
var result = await engine.AskAsync("products by category");

string csv = result.ToCsv();    // Label,Value\nJewelry,5\nAccessories,3\n
string json = result.ToJson();  // { "title": "...", "chartType": "bar", "data": [...] }
```

### Read-Only Connection Validator

NaturalQuery logs a warning at startup if your connection string is not configured for read-only access. This is advisory only and does not block execution.

For SQL Server, add `ApplicationIntent=ReadOnly` to your connection string. For PostgreSQL, use `Target Session Attrs=read-only`. SQLite and Athena are always considered safe.

### Schema Discovery

Auto-discover table schemas from your database instead of defining them manually.

```csharp
// PostgreSQL
.UsePostgresSchemaDiscovery("Host=localhost;Database=mydb;Username=user;Password=pass")

// SQL Server
.UseSqlServerSchemaDiscovery("Server=localhost;Database=mydb;Trusted_Connection=true;")

// SQLite
.UseSqliteSchemaDiscovery("DataSource=mydb.db")

// Then in your app
var discovery = app.Services.GetRequiredService<ISchemaDiscovery>();
var tables = await discovery.DiscoverAsync(schemaFilter: "public");
```

Also supports Athena/Glue catalog discovery via `AthenaSchemaDiscovery`.

### Schema Validation

Validates that SQL references only known tables. Automatically runs during `AskAsync` when tables are configured (warning level, non-blocking).

### SQL Validation

Built-in security — all generated SQL is validated:

- Only `SELECT` and `WITH` (CTE) queries allowed
- Blocks: `DELETE`, `UPDATE`, `INSERT`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `GRANT`, `REVOKE`
- Multi-statement detection (rejects queries with multiple statements separated by semicolons)
- String literals are ignored to prevent false positives
- Tenant isolation enforced when configured
- Custom forbidden keywords via `options.ForbiddenSqlKeywords`

### Transaction Wrapping

Extra safety layer for database executors that support writes. Wraps every query in `BEGIN` + `ROLLBACK` so even if SQL validation is somehow bypassed, nothing gets written.

```csharp
.UsePostgresExecutor("Host=localhost;Database=mydb", wrapInTransaction: true)
.UseSqlServerExecutor("Server=localhost;Database=mydb", wrapInTransaction: true)
```

### Multi-Tenancy

Built-in tenant isolation. Every query MUST contain a `WHERE` filter on the tenant column, and the placeholder is replaced with the actual tenant ID at runtime.

```csharp
options.TenantIdColumn = "tenant_id";
options.TenantIdPlaceholder = "{TENANT_ID}";

var result = await engine.AskAsync("all users", tenantId: "abc-123");
```

### Query Cost Estimation (Athena)

Estimate scan cost before executing.

```csharp
var estimator = app.Services.GetRequiredService<IQueryCostEstimator>();
var cost = await estimator.EstimateAsync(sql);
// cost.EstimatedBytes, cost.EstimatedCostUsd, cost.FormattedSize
```

### Error Handling

Track NL2SQL failures for monitoring and prompt refinement.

```csharp
.UseErrorHandler(async (error, ct) =>
{
    logger.LogError("NL2SQL error: {Type} - {Message}", error.ErrorType, error.Message);
    await myErrorStore.SaveAsync(error);
})
```

`NaturalQueryError` includes: Question, Sql, ErrorType, Message, TenantId, TokensUsed, ElapsedMs, Exception.

### OpenTelemetry Diagnostics

All operations emit `System.Diagnostics.Activity` spans, compatible with any OpenTelemetry exporter.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(NaturalQueryDiagnostics.SourceName));
```

Spans: `NaturalQuery.Ask`, `NaturalQuery.LlmGenerate`, `NaturalQuery.Execute` with tags for tokens, chart type, cache hits, and errors.

## Configuration Reference

```csharp
options.Tables = new List<TableSchema> { ... };       // Database schema
options.TenantIdColumn = "tenant_id";                  // Multi-tenant column
options.TenantIdPlaceholder = "{TENANT_ID}";           // Placeholder in SQL
options.MaxTokens = 1000;                              // LLM max tokens
options.Temperature = 0.1;                             // LLM temperature (0=deterministic)
options.MaxRetries = 2;                                // Auto-retry on query failure (0-3)
options.CacheTtlMinutes = 5;                           // Cache TTL (0=disabled)
options.RateLimitPerMinute = 60;                       // Rate limit per tenant
options.CustomSystemPrompt = "...{TABLES_SCHEMA}...";  // Override entire prompt
options.AdditionalRules = new() { "Use UTC dates" };   // Extra instructions for LLM
options.ForbiddenSqlKeywords = new() { "UNION " };     // Extra blocked SQL keywords
```

## Custom Providers

### Custom LLM Provider

```csharp
public class OllamaProvider : ILlmProvider
{
    public async Task<LlmResponse> GenerateAsync(
        string systemPrompt, string userPrompt, CancellationToken ct)
    {
        // Your implementation
        return new LlmResponse(responseText, tokensUsed);
    }
}

.UseLlmProvider<OllamaProvider>()
```

### Custom Query Executor

```csharp
public class BigQueryExecutor : IQueryExecutor
{
    public async Task<List<DataPoint>> ExecuteChartQueryAsync(string sql, CancellationToken ct)
    {
        // Return label + value pairs
    }

    public async Task<List<Dictionary<string, string>>> ExecuteTableQueryAsync(string sql, CancellationToken ct)
    {
        // Return rows as dictionaries
    }
}

.UseQueryExecutor<BigQueryExecutor>()
```

## Chart Types

The LLM automatically selects the best visualization:

| Type | When | SQL Pattern |
|------|------|-------------|
| `line` | Time series | `SELECT period AS label, COUNT(*) AS value` |
| `bar` | Categories | `SELECT category AS label, COUNT(*) AS value` |
| `bar_horizontal` | Rankings | `... ORDER BY value DESC LIMIT N` |
| `pie` / `donut` | Distributions | `SELECT status AS label, COUNT(*) AS value` |
| `area` | Filled time series | Same as line |
| `metric` | Single number | `SELECT COUNT(*) AS value` |
| `table` | Detailed data | `SELECT col1, col2, col3 ...` |

## Samples

The `samples/` directory contains ready-to-run projects that demonstrate NaturalQuery in different scenarios. Both require an OpenAI API key set as the `OPENAI_API_KEY` environment variable.

### Console App

A minimal console application that creates a SQLite database with sample data, asks a few questions, and prints the generated SQL, chart type, and results.

```bash
cd samples/ConsoleApp
dotnet run
```

### Web API

An ASP.NET minimal API that exposes NaturalQuery as a web endpoint with caching, rate limiting, and the built-in playground UI. Run it and open `http://localhost:5000/playground` in your browser.

```bash
cd samples/WebApi
dotnet run
```

## License

MIT — see [LICENSE](LICENSE).

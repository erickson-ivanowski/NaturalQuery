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

### With OpenAI + PostgreSQL

```csharp
builder.Services.AddNaturalQuery(options =>
{
    options.Tables = new List<TableSchema>
    {
        new("users", new[]
        {
            new ColumnDef("id", "int"),
            new ColumnDef("name", "string"),
            new ColumnDef("email", "string"),
            new ColumnDef("created_at", "timestamp"),
        })
    };
})
.UseOpenAiProvider("sk-your-api-key", model: "gpt-4o-mini")
.UsePostgresExecutor("Host=localhost;Database=mydb;Username=user;Password=pass");
```

### Usage

```csharp
app.MapGet("/ask", async (string question, INaturalQueryEngine engine) =>
{
    var result = await engine.AskAsync(question, tenantId: "my-tenant");
    return Results.Ok(result);
});
```

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
IQueryExecutor (Athena / PostgreSQL / custom)
    ↓
QueryResult (data + chart type + metadata)
    ↓
[Cache store] + [Diagnostics]
```

## Features

### Providers

| Provider | Component | Description |
|----------|-----------|-------------|
| AWS Bedrock (Claude) | `ILlmProvider` | Built-in, uses Converse API |
| OpenAI / compatible | `ILlmProvider` | Built-in, raw HttpClient (no SDK needed) |
| Amazon Athena | `IQueryExecutor` | Built-in, async polling |
| PostgreSQL | `IQueryExecutor` | Built-in, via Npgsql |
| Custom | Both | Implement `ILlmProvider` or `IQueryExecutor` |

### Query Cache

Avoid redundant LLM calls for repeated questions:

```csharp
.UseInMemoryCache()

// Configure TTL
options.CacheTtlMinutes = 10;  // default: 5
```

Cache key is a SHA256 hash of `(question + tenantId)` for privacy. Implement `IQueryCache` for Redis/distributed caching.

### Rate Limiting

Per-tenant sliding window rate limiter:

```csharp
.UseInMemoryRateLimiter()

// Configure limit
options.RateLimitPerMinute = 30;  // default: 60
```

Implement `IRateLimiter` for distributed rate limiting.

### Conversation Context (Follow-up Questions)

Support "now filter by cancelled ones" style follow-up questions:

```csharp
var context = new ConversationContext();

var result1 = await engine.AskAsync("all orders this month", tenantId: "t1", context: context);
// context now contains the first question + SQL

var result2 = await engine.AskAsync("now only cancelled ones", tenantId: "t1", context: context);
// LLM receives conversation history and generates a filtered query
```

### Query Explanation

Let users understand what the generated SQL does:

```csharp
var result = await engine.InterpretAsync("top products by revenue");
var explanation = await engine.ExplainAsync(result.Sql);
// "This query groups products by name and sums their revenue, returning the top results."
```

### Suggested Questions

Help users discover what they can ask:

```csharp
var suggestions = await engine.SuggestQuestionsAsync(count: 5);
// ["What are the top 10 products by sales?", "How many orders per month?", ...]
```

### Error Handling

Track NL2SQL failures for monitoring and prompt refinement:

```csharp
// Inline handler
.UseErrorHandler(async (error, ct) =>
{
    logger.LogError("NL2SQL error: {Type} - {Message}", error.ErrorType, error.Message);
    await myErrorStore.SaveAsync(error);
})

// Or implement IErrorHandler
.UseErrorHandler<MyErrorHandler>()
```

`NaturalQueryError` includes: Question, Sql, ErrorType, Message, TenantId, TokensUsed, ElapsedMs, Exception.

### OpenTelemetry Diagnostics

All operations emit `System.Diagnostics.Activity` spans, compatible with any OpenTelemetry exporter:

```csharp
// In your OpenTelemetry config
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(NaturalQueryDiagnostics.SourceName));
```

Spans: `NaturalQuery.Ask`, `NaturalQuery.LlmGenerate`, `NaturalQuery.Execute` with tags for tokens, chart type, cache hits, and errors.

### Schema Discovery

Auto-discover table schemas from your database:

```csharp
// PostgreSQL
.UsePostgresSchemaDiscovery("Host=localhost;Database=mydb;Username=user;Password=pass")

// Then in your app
var discovery = app.Services.GetRequiredService<ISchemaDiscovery>();
var tables = await discovery.DiscoverAsync(schemaFilter: "public");
```

Also supports Athena/Glue catalog discovery via `AthenaSchemaDiscovery`.

### Schema Validation

Validates that SQL references only known tables:

```csharp
var error = SchemaValidator.ValidateColumns(sql, options.Tables);
// null = valid, or "Unknown table(s): foo. Available: users, orders"
```

Automatically runs during `AskAsync` when tables are configured (warning level, non-blocking).

### Query Cost Estimation (Athena)

Estimate scan cost before executing:

```csharp
var estimator = app.Services.GetRequiredService<IQueryCostEstimator>();
var cost = await estimator.EstimateAsync(sql);
// cost.EstimatedBytes, cost.EstimatedCostUsd, cost.FormattedSize
```

### SQL Validation

Built-in security — all generated SQL is validated:

- Only `SELECT` and `WITH` (CTE) queries allowed
- Blocks: `DELETE`, `UPDATE`, `INSERT`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `GRANT`, `REVOKE`
- Multi-statement detection (rejects queries with multiple statements separated by semicolons)
- String literals are ignored to prevent false positives
- Tenant isolation enforced when configured
- Custom forbidden keywords via `options.ForbiddenSqlKeywords`

### Transaction Wrapping (PostgreSQL)

Extra safety layer for database executors that support writes. Wraps every query in `BEGIN` + `ROLLBACK` so even if SQL validation is somehow bypassed, nothing gets written:

```csharp
.UsePostgresExecutor("Host=localhost;Database=mydb", wrapInTransaction: true)
```

Basically free for SELECT queries. Not needed for read-only engines like Athena.

### Multi-Tenancy

Built-in tenant isolation:

```csharp
options.TenantIdColumn = "tenant_id";
options.TenantIdPlaceholder = "{TENANT_ID}";

// Every query MUST contain WHERE tenant_id = '{TENANT_ID}'
// The placeholder is replaced with the actual tenant ID at runtime
var result = await engine.AskAsync("all users", tenantId: "abc-123");
```

## Configuration Reference

```csharp
options.Tables = new List<TableSchema> { ... };       // Database schema
options.TenantIdColumn = "tenant_id";                  // Multi-tenant column
options.TenantIdPlaceholder = "{TENANT_ID}";           // Placeholder in SQL
options.MaxTokens = 1000;                              // LLM max tokens
options.Temperature = 0.1;                             // LLM temperature (0=deterministic)
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

## License

MIT — see [LICENSE](LICENSE).

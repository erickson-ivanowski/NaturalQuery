# Contract: Public API Surface (additive, v2.1.0)

**Feature**: `001-security-audit-enhancements`

Rule: every item below is NEW. Nothing existing changes signature, behavior default, or removal. Existing suite must compile and pass unmodified (SC-003).

## Engine (INaturalQueryEngine — default interface members to avoid breaking implementors)

```csharp
Task<QueryResult> AskPagedAsync(string question, int page, int pageSize,
    string? tenantId = null, ConversationContext? context = null, CancellationToken ct = default);

Task<QueryPreview> PreviewAsync(string question, string? tenantId = null,
    ConversationContext? context = null, CancellationToken ct = default);

Task<QueryResult> ExecuteApprovedAsync(string sql, string? tenantId = null,
    CancellationToken ct = default); // re-validates everything at execution time
```

## Service registration (NaturalQueryBuilder / IServiceCollection extensions)

```csharp
.UseAnthropicProvider(string apiKey, string model = "claude-sonnet-5", ...)
.UseOpenRouterProvider(string apiKey, string model, string? referer = null, string? title = null)
.UseMySqlExecutor(string connectionString, bool wrapInTransaction = false)
.UseMySqlSchemaDiscovery(string connectionString)
.UseAuditSink(IAuditSink sink)                    // + overloads: factory, delegate
.UseSemanticCache(Action<SemanticCacheOptions>? configure = null)  // requires an IEmbeddingProvider
.UseOpenAiEmbeddings(string apiKey, string model = "text-embedding-3-small")
.UseBedrockEmbeddings(string modelId = "amazon.titan-embed-text-v2:0")
.AddNaturalQueryHealthChecks(this IHealthChecksBuilder builder, ...)
```

`AddNaturalQuery(...)` additionally registers `IValidateOptions<NaturalQueryOptions>` with ValidateOnStart semantics (fail-fast only for impossible configs — FR-028).

## Endpoints

```csharp
app.MapNaturalQuery("/ask");                                  // existing — unchanged behavior
app.MapNaturalQuery("/ask", new NaturalQueryEndpointOptions   // NEW overload
{
    RequireAuthorization = true,          // host auth; or:
    AuthorizationPolicy  = "nlq-policy",  // named policy
    MaxQuestionLength    = 2000,
    MaxContextTurns      = 20,
});
// NEW routes under the same prefix:
// POST {prefix}/preview  → QueryPreview (no execution)
// POST {prefix}/execute  → QueryResult  (body: { sql, tenantId }; full re-validation)
// GET/POST pagination:  ?page=&pageSize=  /  { "page": n, "pageSize": m }
```

Error contract (all endpoints, FR-010): failures return `{ "error": "<safe generic message>", "correlationId": "<id>" }`; internal detail only in server logs keyed by correlationId.

```csharp
app.MapNaturalQueryPlayground("/playground", apiPath: "/ask",
    allowInProduction: false);            // NEW optional param; default refuses outside Development
```

## Options (NaturalQueryOptions — new properties, defaults preserve behavior)

```csharp
options.MaxQuestionLength = 2000;
options.MaxContextTurns = 20;
options.MaxResultRows = 10_000;
options.QueryTimeoutSeconds = 30;
options.TenantIdPattern = "^[A-Za-z0-9._-]{1,128}$";
options.InjectionScreening = InjectionScreeningMode.Warn;   // Off | Warn | Block
options.InjectionPatterns.Add("...");                        // extends built-ins
options.SemanticCacheSimilarityThreshold = 0.97;
```

## Models

```csharp
new ColumnDef("email", "string", "customer email", sensitive: true);  // NEW overload
result.Truncated;         // bool — row cap applied
result.CorrelationId;     // string?
result.InjectionFlagged;  // bool — warn-mode screening hit
```

## Exports

```csharp
byte[] xlsx = result.ToExcel();
Stream s   = result.ToExcelStream();
```

## NaturalQuery.Redis (new package, version-locked to core)

```csharp
.UseRedisCache("localhost:6379")          // IQueryCache — unavailable store ⇒ cache miss (fail-open)
.UseRedisRateLimiter("localhost:6379")    // IRateLimiter — unavailable store ⇒ deny (fail-closed, logged)
```

## Behavioral contract deltas (documented security fixes — the only permitted behavior changes)

| # | Previously accepted | Now |
|---|---------------------|-----|
| 1 | Obfuscated write SQL (`DELETE/**/FROM`, newline-split keywords, MERGE/EXEC/ATTACH/COPY/SELECT INTO…) | Rejected (FR-001/FR-002) |
| 2 | Tenant IDs with quotes/comment markers/whitespace | Rejected before any processing (FR-006) |
| 3 | Tenant value present only in a literal/comment | Rejected — must be a real filter on the tenant column (FR-007) |
| 4 | Unlimited question / context size on endpoints | 413-style rejection over configured caps (FR-009) |
| 5 | Raw internal error text in endpoint responses | Safe message + correlationId (FR-010) |
| 6 | Playground served in any environment | Development-only unless opted in (FR-012) |
| 7 | Caller-supplied context turns with dangerous SQL | Rejected (FR-013) |
| 8 | Unbounded rate-limiter tenant map | Idle entries evicted (FR-015) |
| 9 | Unbounded rows / unbounded execution time | Capped + truncation marker; 30s default timeout (FR-016/FR-017) |

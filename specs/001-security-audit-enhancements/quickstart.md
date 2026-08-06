# Quickstart: NaturalQuery 2.1 — Hardened Setup

All-protections configuration in under 10 lines (SC-007):

```csharp
builder.Services.AddNaturalQuery(options =>
{
    options.Tables = tables;                       // mark PII: new ColumnDef("email", "string", sensitive: true)
    options.TenantIdColumn = "tenant_id";
    options.TenantIdPlaceholder = "{TENANT_ID}";
    options.InjectionScreening = InjectionScreeningMode.Block;  // strict mode (default: Warn)
})
.UseAnthropicProvider(builder.Configuration["Anthropic:ApiKey"]!)
.UseMySqlExecutor(connString, wrapInTransaction: true)
.UseInMemoryCache().UseInMemoryRateLimiter()
.UseAuditSink(async (record, ct) => await auditStore.SaveAsync(record));

builder.Services.AddHealthChecks().AddNaturalQueryHealthChecks();

app.MapNaturalQuery("/ask", new NaturalQueryEndpointOptions { RequireAuthorization = true });
app.MapNaturalQueryPlayground("/playground");     // dev-only automatically
```

What you get, out of the box (no config needed):

- Hardened SQL validation — obfuscated/dialect write operations rejected
- Tenant IDs validated (`^[A-Za-z0-9._-]{1,128}$`), tenant filter structurally verified
- Question ≤ 2,000 chars, history ≤ 20 turns, results ≤ 10,000 rows (`result.Truncated`), 30s query timeout
- Safe endpoint errors with `correlationId`; details only in server logs
- Prompt-injection detection (warn + flag by default)
- Startup config validation — impossible configs fail at boot with the exact reason

Opt-ins:

```csharp
// OpenRouter (any catalog model) — reuses OpenAI-compatible provider
.UseOpenRouterProvider(key, "deepseek/deepseek-chat")

// Multi-instance deployments (NaturalQuery.Redis package)
.UseRedisCache("redis:6379").UseRedisRateLimiter("redis:6379")

// Semantic cache — paraphrases reuse results, per tenant
.UseOpenAiEmbeddings(key).UseSemanticCache()

// Preview / approve flow
var preview = await engine.PreviewAsync("delete-looking question?", tenantId);
var result  = await engine.ExecuteApprovedAsync(preview.Sql, tenantId);  // re-validated

// Pagination & Excel
var page2 = await engine.AskPagedAsync("all orders", page: 2, pageSize: 500, tenantId);
File.WriteAllBytes("report.xlsx", result.ToExcel());
```

Upgrade note (2.0 → 2.1): additive release. Only behavior change: dangerous inputs that previously slipped through are now rejected — see CHANGELOG "Security fixes".

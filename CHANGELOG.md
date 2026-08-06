# Changelog

All notable changes to NaturalQuery are documented here.

## 2.1.0

Security audit and hardening release. Strictly additive — no breaking changes to existing public API, configuration defaults, or observable behavior for legitimate inputs. The only permitted behavior delta is the rejection of genuinely dangerous inputs that previously slipped through; those are documented below as security fixes.

### Security fixes

- **SQL validation hardened**: `SqlValidator` now runs a normalization pipeline (comment stripping under both nesting and non-nesting block-comment dialects, string-literal removal, whitespace collapse) before a word-boundary keyword check. Obfuscated writes (`DELETE/**/FROM`, newline-split keywords, casing tricks) and a much larger set of dangerous operations (MERGE, EXEC/EXECUTE, CALL, ATTACH/DETACH, PRAGMA, COPY, VACUUM, REINDEX, LOAD, `SELECT ... INTO`, `xp_`/`sp_` prefixes, OPENROWSET/OPENQUERY) are now rejected. `SqlValidator.Validate`'s public signature is unchanged.
- **Tenant isolation hardened**: tenant identifiers are now validated against a character-policy regex (default `^[A-Za-z0-9._-]{1,128}$`, operator-configurable) before any use — quote characters, comment markers, and query syntax in a tenant ID are rejected before a query is ever built. Additionally, the tenant value must now be structurally verified as a real equality filter on the configured tenant column — a tenant value present only in a comment or unrelated literal is rejected. Single-tenant configurations (no tenant column configured) are unaffected.
- **Endpoint protections**: the `MapNaturalQuery` overload accepting `NaturalQueryEndpointOptions` enforces question-length and history-turn caps before any AI call, returns safe generic errors with a correlation ID (full detail only in server logs), and supports host-delegated `RequireAuthorization`. The existing no-arguments overload keeps its current behavior except that generous default input limits now apply.
- **Playground production guard**: `MapNaturalQueryPlayground` now refuses to serve outside `Development` unless `allowInProduction: true` is passed explicitly, logging a warning when refused.
- **Prompt-injection screening**: questions and caller-supplied conversation-history turns are screened against known instruction-override patterns (English, Portuguese, Spanish; operator-extensible). Default mode (`Warn`) logs and flags without refusing; `Block` mode refuses matched questions before any AI call. Conversation-history turns are additionally screened with the same hardened SQL validator, unconditionally, regardless of injection-screening mode.
- **Resource governance**: `InMemoryRateLimiter` now evicts idle tenant entries once tracked tenants exceed a soft cap, keeping memory bounded under unbounded distinct tenant floods without ever dropping an active tenant's window mid-minute. All built-in query executors now enforce a configurable maximum row count (`MaxResultRows`, default 10,000) with a `Truncated` marker on the result. The engine enforces a configurable execution timeout (`QueryTimeoutSeconds`, default 30s) independent of the caller's own cancellation token.

### Added

- **Audit trail**: opt-in `IAuditSink` (`UseAuditSink`) receives exactly one `AuditRecord` per processed question (success or failure) with question, generated SQL, tenant, outcome, duration, and token usage. Sink failures never fail the user's request.
- **Sensitive data masking**: mark a column `sensitive: true` on `ColumnDef` to fully redact (`***`) its values in every output form (table data, chart data, and all exports).
- **MySQL/MariaDB support**: `UseMySqlExecutor` and `UseMySqlSchemaDiscovery`, with the same safety guarantees as the existing executors. `MySqlConnector` is the only new dependency added to the core package.
- **Direct Anthropic provider**: `UseAnthropicProvider` calls the Anthropic Messages API directly via raw `HttpClient` — same response contract, token reporting, and error classification as the existing providers.
- **OpenRouter convenience**: `UseOpenRouterProvider` reuses the existing OpenAI-compatible provider with OpenRouter's base URL and recommended attribution headers — no new provider implementation.
- **`NaturalQuery.Redis` companion package**: `UseRedisCache` and `UseRedisRateLimiter` for multi-instance deployments sharing one cache and one combined per-tenant rate limit. The Redis cache fails open (miss) and the Redis rate limiter fails closed (deny) when the store is unavailable, with the condition logged.
- **Health checks**: `AddNaturalQueryHealthChecks()` (alias `AddNaturalQueryHealthCheck()`) reports query-executor and AI-provider reachability without making a billable AI call.
- **Native metrics**: a `"NaturalQuery"` `System.Diagnostics.Metrics.Meter` emits query counts, duration, token usage, and cache hit/miss — consumable by any standard .NET metrics pipeline (OpenTelemetry, `dotnet-counters`, etc.).
- **Startup configuration validation**: `IValidateOptions<NaturalQueryOptions>` runs on `ValidateOnStart`, failing fast with a message naming the exact problem (e.g. tenant column configured without a placeholder, or vice versa; non-positive limits; an out-of-range semantic-cache threshold). Every currently-valid configuration keeps starting unchanged.
- **Excel export**: `QueryResult.ToExcel()` / `.ToExcelStream()` produce a minimal `.xlsx` file via a hand-rolled `System.IO.Compression.ZipArchive`-based writer — no third-party Excel dependency.
- **Semantic cache**: opt-in `UseSemanticCache()` (with `UseOpenAiEmbeddings` or `UseBedrockEmbeddings`) reuses results for questions with equivalent meaning, strictly scoped per tenant, with a conservative similarity threshold (default 0.97) so a wrong reuse is treated as a defect, not a tradeoff.
- **Pagination**: `AskPagedAsync` slices a captured result set with stable ordering and no additional AI call per page. Available via the engine API and as `page`/`pageSize` parameters on the mapped HTTP endpoints.
- **Preview / approve flow**: `PreviewAsync` returns the generated, validated query and its estimated cost (when an `IQueryCostEstimator` is registered) without executing it. `ExecuteApprovedAsync` re-applies every safety rule — hardened validation, tenant-filter verification, masking, and caps — at execution time, so a stale approval is rejected rather than silently trusted. Exposed via `POST {prefix}/preview` and `POST {prefix}/execute` on the mapped HTTP endpoints.

### New configuration options (all additive, defaults preserve current behavior)

`NaturalQueryOptions`: `MaxQuestionLength` (2000), `MaxContextTurns` (20), `MaxResultRows` (10000), `QueryTimeoutSeconds` (30), `TenantIdPattern` (`^[A-Za-z0-9._-]{1,128}$`), `InjectionScreening` (`Warn`), `InjectionPatterns`, `SemanticCacheSimilarityThreshold` (0.97).

`QueryResult`: `Truncated`, `CorrelationId`, `InjectionFlagged`.

`ColumnDef`: `Sensitive` (via a new constructor overload; existing constructors unchanged).
